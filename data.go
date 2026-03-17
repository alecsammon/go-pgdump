package pgdump

import (
	"database/sql"
	"fmt"
	"strings"
)

// options for dumping selective tables.
type TableOptions struct {
	TableSuffix string
	TablePrefix string
	Schema      string
	SchemaOnly  bool // When true, only dump table definitions without row data
}

// tableInfo holds metadata about a table for partition-aware dumping.
type tableInfo struct {
	Name          string
	IsPartitioned bool // true if this is a partitioned parent (relkind = 'p')
	IsPartition   bool // true if this is a child partition
	ParentName    string
}

// returns table metadata for all tables matching options.
// Categorises tables as regular, partitioned parents, or partition children.
func getTables(db *sql.DB, opts *TableOptions) ([]tableInfo, error) {
	schema := "public"
	likePattern := "%%"
	if opts != nil {
		if opts.Schema != "" {
			schema = opts.Schema
		}
		likePattern = opts.TablePrefix + "%%" + opts.TableSuffix
	}

	query := fmt.Sprintf(`
SELECT c.relname,
       c.relkind,
       COALESCE(parent.relname, '') AS parent_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_inherits inh ON inh.inhrelid = c.oid
LEFT JOIN pg_class parent ON parent.oid = inh.inhparent
WHERE n.nspname = '%s'
  AND c.relkind IN ('r', 'p')
  AND c.relname LIKE '%s'
ORDER BY c.relname;`, schema, likePattern)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []tableInfo
	for rows.Next() {
		var name, relkind, parentName string
		if err := rows.Scan(&name, &relkind, &parentName); err != nil {
			return nil, err
		}
		ti := tableInfo{
			Name:          name,
			IsPartitioned: relkind == "p",
			IsPartition:   parentName != "",
			ParentName:    parentName,
		}
		if schema != "public" {
			ti.Name = schema + "." + ti.Name
			if ti.ParentName != "" {
				ti.ParentName = schema + "." + ti.ParentName
			}
		}
		tables = append(tables, ti)
	}
	return tables, nil
}

// getPartitionBound returns the partition bound expression for a child partition.
func getPartitionBound(db *sql.DB, tableName string) (string, error) {
	query := `
SELECT pg_get_expr(c.relpartbound, c.oid)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = $1 AND n.nspname = 'public';`

	var bound string
	if err := db.QueryRow(query, tableName).Scan(&bound); err != nil {
		return "", fmt.Errorf("error querying partition bound for %s: %w", tableName, err)
	}
	return bound, nil
}

// getPartitionStrategy returns the PARTITION BY clause for a partitioned parent table.
func getPartitionStrategy(db *sql.DB, tableName string) (string, error) {
	query := `
SELECT
    CASE p.partstrat
        WHEN 'r' THEN 'RANGE'
        WHEN 'l' THEN 'LIST'
        WHEN 'h' THEN 'HASH'
    END AS strategy,
    string_agg(a.attname, ', ' ORDER BY pos.ordinality) AS columns
FROM pg_partitioned_table p
JOIN pg_class c ON c.oid = p.partrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN LATERAL unnest(p.partattrs::int[]) WITH ORDINALITY AS pos(attnum, ordinality) ON true
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = pos.attnum
WHERE c.relname = $1 AND n.nspname = 'public'
GROUP BY p.partstrat;`

	var strategy, columns string
	if err := db.QueryRow(query, tableName).Scan(&strategy, &columns); err != nil {
		return "", fmt.Errorf("error querying partition strategy for %s: %w", tableName, err)
	}
	return fmt.Sprintf("PARTITION BY %s (%s)", strategy, columns), nil
}

// getEnumTypes queries the database for all user-defined enum types and returns
// the SQL statements to recreate them.
func getEnumTypes(db *sql.DB) (string, error) {
	query := `
SELECT t.typname AS enum_name,
       string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) AS enum_values
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = 'public'
GROUP BY t.typname
ORDER BY t.typname;`

	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying enum types: %w", err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var enumName, enumValues string
		if err := rows.Scan(&enumName, &enumValues); err != nil {
			return "", fmt.Errorf("error scanning enum type: %w", err)
		}
		labels := strings.Split(enumValues, ",")
		for i, l := range labels {
			labels[i] = "'" + l + "'"
		}
		sb.WriteString(fmt.Sprintf(
			"CREATE TYPE %s AS ENUM (%s);\n\n",
			escapeReservedName(enumName),
			strings.Join(labels, ", "),
		))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating enum types: %w", err)
	}

	return sb.String(), nil
}

// getColumnDefinitions returns the column definitions for a table, including
// NOT NULL constraints and DEFAULT values.
func getColumnDefinitions(db *sql.DB, tableName string) ([]string, error) {
	query := `
SELECT
    c.column_name,
    c.data_type,
    c.udt_name,
    c.character_maximum_length,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
WHERE c.table_name = $1 AND c.table_schema = 'public'
ORDER BY c.ordinal_position;`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName, dataType, udtName, isNullable string
		var charMaxLength *int
		var columnDefault *string
		if err := rows.Scan(&columnName, &dataType, &udtName, &charMaxLength, &isNullable, &columnDefault); err != nil {
			return nil, err
		}

		switch dataType {
		case "USER-DEFINED":
			dataType = udtName
		case "ARRAY":
			// udt_name for arrays is prefixed with underscore, e.g. "_text" for text[]
			dataType = strings.TrimPrefix(udtName, "_") + "[]"
		}

		columnDef := fmt.Sprintf("%s %s", columnName, dataType)
		if charMaxLength != nil {
			columnDef += fmt.Sprintf("(%d)", *charMaxLength)
		}
		if columnDefault != nil {
			columnDef += " DEFAULT " + *columnDefault
		}
		if isNullable == "NO" {
			columnDef += " NOT NULL"
		}
		columns = append(columns, columnDef)
	}
	return columns, nil
}

// generates the SQL for creating a regular or partitioned parent table.
func getCreateTableStatement(db *sql.DB, table tableInfo) (string, error) {
	columns, err := getColumnDefinitions(db, table.Name)
	if err != nil {
		return "", err
	}

	suffix := ""
	if table.IsPartitioned {
		partClause, err := getPartitionStrategy(db, table.Name)
		if err != nil {
			return "", err
		}
		suffix = " " + partClause
	}

	return fmt.Sprintf(
		"CREATE TABLE %s (\n    %s\n)%s;",
		escapeReservedName(table.Name),
		strings.Join(columns, ",\n    "),
		suffix,
	), nil
}

// getCreatePartitionStatement generates CREATE TABLE ... PARTITION OF ... FOR VALUES ...
func getCreatePartitionStatement(db *sql.DB, table tableInfo) (string, error) {
	bound, err := getPartitionBound(db, table.Name)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"CREATE TABLE %s PARTITION OF %s %s;",
		escapeReservedName(table.Name),
		escapeReservedName(table.ParentName),
		bound,
	), nil
}

// generates the COPY command to import data for a table.
func getTableDataCopyFormat(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s", escapeReservedName(tableName))
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var dataRows []string
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return "", err
		}
		var valueStrings []string
		for _, value := range values {
			if value == nil {
				valueStrings = append(valueStrings, "\\N")
			} else {
				valueStrings = append(valueStrings, string(value))
			}
		}
		dataRows = append(dataRows, strings.Join(valueStrings, "\t"))
	}

	// Skip COPY block entirely if there are no rows
	if len(dataRows) == 0 {
		return "", nil
	}

	var output strings.Builder
	output.WriteString(fmt.Sprintf(
		"COPY %s (%s) FROM stdin;\n",
		escapeReservedName(tableName),
		strings.Join(columns, ", "),
	))
	for _, row := range dataRows {
		output.WriteString(row + "\n")
	}
	output.WriteString("\\.\n")

	return output.String(), nil
}

// scriptComments generates COMMENT ON statements for a table and its columns.
func scriptComments(db *sql.DB, tableName string) (string, error) {
	var sb strings.Builder

	// Table comment
	var tableComment *string
	err := db.QueryRow(`
SELECT obj_description(c.oid)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = $1 AND n.nspname = 'public';`, tableName).Scan(&tableComment)
	if err != nil {
		return "", fmt.Errorf("error querying table comment for %s: %w", tableName, err)
	}
	if tableComment != nil && *tableComment != "" {
		sb.WriteString(fmt.Sprintf(
			"COMMENT ON TABLE %s IS '%s';\n",
			escapeReservedName(tableName),
			strings.ReplaceAll(*tableComment, "'", "''"),
		))
	}

	// Column comments
	rows, err := db.Query(`
SELECT a.attname, col_description(a.attrelid, a.attnum)
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = $1
  AND n.nspname = 'public'
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND col_description(a.attrelid, a.attnum) IS NOT NULL
ORDER BY a.attnum;`, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying column comments for %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colComment string
		if err := rows.Scan(&colName, &colComment); err != nil {
			return "", fmt.Errorf("error scanning column comment: %w", err)
		}
		sb.WriteString(fmt.Sprintf(
			"COMMENT ON COLUMN %s.%s IS '%s';\n",
			escapeReservedName(tableName),
			escapeReservedName(colName),
			strings.ReplaceAll(colComment, "'", "''"),
		))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating column comments: %w", err)
	}

	return sb.String(), nil
}

// getExtensions returns CREATE EXTENSION statements for all installed extensions
// that are not default PostgreSQL extensions.
func getExtensions(db *sql.DB) (string, error) {
	query := `
SELECT extname
FROM pg_extension
WHERE extname NOT IN ('plpgsql')
ORDER BY extname;`

	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying extensions: %w", err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var extName string
		if err := rows.Scan(&extName); err != nil {
			return "", fmt.Errorf("error scanning extension: %w", err)
		}
		sb.WriteString(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;\n", escapeReservedName(extName)))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating extensions: %w", err)
	}
	return sb.String(), nil
}

// scriptIndexes returns CREATE INDEX statements for all non-primary-key indexes on a table.
func scriptIndexes(db *sql.DB, tableName string) (string, error) {
	query := `
SELECT indexdef
FROM pg_indexes
WHERE tablename = $1
  AND schemaname = 'public'
  AND indexname NOT IN (
    SELECT conname FROM pg_constraint
    WHERE conrelid = (
      SELECT oid FROM pg_class WHERE relname = $1
        AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    )
    AND contype IN ('p', 'u')
  )
ORDER BY indexname;`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying indexes for %s: %w", tableName, err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var indexDef string
		if err := rows.Scan(&indexDef); err != nil {
			return "", fmt.Errorf("error scanning index: %w", err)
		}
		sb.WriteString(indexDef + ";\n")
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating indexes: %w", err)
	}
	return sb.String(), nil
}

// scriptForeignKeys returns ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY statements.
func scriptForeignKeys(db *sql.DB, tableName string) (string, error) {
	query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = con.connamespace
WHERE con.contype = 'f'
  AND rel.relname = $1
  AND nsp.nspname = 'public'
ORDER BY con.conname;`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying foreign keys for %s: %w", tableName, err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var conName, conDef string
		if err := rows.Scan(&conName, &conDef); err != nil {
			return "", fmt.Errorf("error scanning foreign key: %w", err)
		}
		sb.WriteString(fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s %s;\n",
			escapeReservedName(tableName),
			escapeReservedName(conName),
			conDef,
		))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating foreign keys: %w", err)
	}
	return sb.String(), nil
}

// scriptCheckConstraints returns ALTER TABLE ... ADD CONSTRAINT ... CHECK statements.
func scriptCheckConstraints(db *sql.DB, tableName string) (string, error) {
	query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = con.connamespace
WHERE con.contype = 'c'
  AND rel.relname = $1
  AND nsp.nspname = 'public'
ORDER BY con.conname;`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying check constraints for %s: %w", tableName, err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var conName, conDef string
		if err := rows.Scan(&conName, &conDef); err != nil {
			return "", fmt.Errorf("error scanning check constraint: %w", err)
		}
		sb.WriteString(fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s %s;\n",
			escapeReservedName(tableName),
			escapeReservedName(conName),
			conDef,
		))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating check constraints: %w", err)
	}
	return sb.String(), nil
}

// scriptUniqueConstraints returns ALTER TABLE ... ADD CONSTRAINT ... UNIQUE statements.
func scriptUniqueConstraints(db *sql.DB, tableName string) (string, error) {
	query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = con.connamespace
WHERE con.contype = 'u'
  AND rel.relname = $1
  AND nsp.nspname = 'public'
ORDER BY con.conname;`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying unique constraints for %s: %w", tableName, err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var conName, conDef string
		if err := rows.Scan(&conName, &conDef); err != nil {
			return "", fmt.Errorf("error scanning unique constraint: %w", err)
		}
		sb.WriteString(fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s %s;\n",
			escapeReservedName(tableName),
			escapeReservedName(conName),
			conDef,
		))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating unique constraints: %w", err)
	}
	return sb.String(), nil
}

func getTableDataAsCSV(db *sql.DB, tableName string) ([][]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s", escapeReservedName(tableName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	output := [][]string{columns}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		var valueStrings []string
		for _, value := range values {
			if value == nil {
				valueStrings = append(valueStrings, "NULL")
			} else {
				valueStrings = append(valueStrings, string(value))
			}
		}
		output = append(output, valueStrings)
	}

	return output, nil
}
