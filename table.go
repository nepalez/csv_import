package main

import (
	"fmt"
	"strings"
)

type Table struct {
	Name         string
	TypedColumns []string
	IndexBy      []string
}

// List of query builders

func (table *Table) DropTemporaryTableQuery() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", table.temporaryName())
}

func (table *Table) CreateTemporaryTableQuery() string {
	return fmt.Sprintf(
		"CREATE TEMPORARY TABLE IF NOT EXISTS %s (%s);",
		table.temporaryName(),
		strings.Join(table.VarcharColumns(), ", "),
	)
}

func (table *Table) UploadFileQuery(file ImportedFile) string {
	return file.UploadQuery(table.temporaryName())
}

func (table *Table) DropTargetTableQuery() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", table.targetName())
}

func (table *Table) CreateTargetTableQuery() string {
	return fmt.Sprintf(
		"CREATE UNLOGGED TABLE IF NOT EXISTS %s (%s);",
		table.targetName(),
		strings.Join(table.TypedColumns, ", "),
	)
}

func (table *Table) CreateTargetIndexQuery() string {
	return fmt.Sprintf(
		"CREATE UNIQUE INDEX BY %s (%s);",
		table.targetName(),
		strings.Join(table.IndexBy, ", "),
	)
}

func (table *Table) PopulateTargetTableQuery() string {
	return fmt.Sprintf("WITH inserted_rows AS (INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT DO NOTHING RETURNING 1) SELECT COUNT(1) AS count FROM inserted_rows;",
		table.targetName(),
		strings.Join(table.PlainColumns(), ", "),
		strings.Join(table.TypedColumns, ", "),
		table.temporaryName(),
	)
}

func (table *Table) AnalyzeTargetTableQuery() string {
	return fmt.Sprintf("ANALYZE %s;", table.targetName())
}

func (table *Table) SwitchTablesQuery() string {
	return fmt.Sprintf(`BEGIN;
		DROP TABLE IF EXISTS %s;
		ALTER TABLE %s RENAME TO %s;
		COMMIT;`,
		table.Name,
		table.targetName(),
		table.Name,
	)
}

// Helpers

func (table *Table) temporaryName() string {
	return fmt.Sprintf("tmp_%s", table.Name)
}

func (table *Table) targetName() string {
	return fmt.Sprintf("target_%s", table.Name)
}

func (table *Table) PlainColumns() []string {
	result := []string{}
	for _, column := range table.TypedColumns {
		list := strings.Split(column, "::")
		result = append(result, list[0])
	}
	return result
}

func (table *Table) VarcharColumns() []string {
	result := []string{}
	for _, column := range table.PlainColumns() {
		result = append(result, fmt.Sprintf("%s::varchar", column))
	}
	return result
}
