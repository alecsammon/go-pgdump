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
}

// returns a slice of table names matching options, if left blank will default to :
//
//	-> no prefix or suffix
//	-> public schema
func getTables(db *sql.DB, opts *TableOptions) ([]string, error) {
	var (
		query string
	)
	if opts != nil {
		if opts.Schema == "" {
			opts.Schema = "public"
		}
		query = fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_name LIKE '%s'", opts.Schema, (opts.TablePrefix + "%%" + opts.TableSuffix))
	} else {
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		if opts.Schema != "public" {
			tables = append(tables, opts.Schema+"."+tableName)
		} else {
			tables = append(tables, tableName)
		}
	}
	return tables, nil
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

// generates the SQL for creating a table, including column definitions.
func getCreateTableStatement(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf(
		"SELECT column_name, data_type, udt_name, character_maximum_length FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position",
		tableName,
	)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName, dataType, udtName string
		var charMaxLength *int
		if err := rows.Scan(&columnName, &dataType, &udtName, &charMaxLength); err != nil {
			return "", err
		}

		// When data_type is USER-DEFINED, use the actual type name from udt_name
		if dataType == "USER-DEFINED" {
			dataType = udtName
		}

		columnDef := fmt.Sprintf("%s %s", columnName, dataType)
		if charMaxLength != nil {
			columnDef += fmt.Sprintf("(%d)", *charMaxLength)
		}
		columns = append(columns, columnDef)
	}

	return fmt.Sprintf(
		"CREATE TABLE %s (\n    %s\n);",
		escapeReservedName(tableName),
		strings.Join(columns, ",\n    "),
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
