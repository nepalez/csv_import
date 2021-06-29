package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func prepareTable() *Table {
	return &Table{
		Name:         "users",
		TypedColumns: []string{"name::text", "email::text"},
		IndexBy:      []string{"email", "name"},
	}
}

func TestDropTemporaryTableQuery(t *testing.T) {
	require.Equal(
		t,
		"DROP TABLE IF EXISTS tmp_users;",
		prepareTable().DropTemporaryTableQuery(),
	)
}

func TestCreateTemporaryTableQuery(t *testing.T) {
	require.Equal(
		t,
		"CREATE TEMPORARY TABLE IF NOT EXISTS tmp_users (name::varchar, email::varchar);",
		prepareTable().CreateTemporaryTableQuery(),
	)
}

func TestUploadFileQueryWithS3File(t *testing.T) {
	file := S3File{
		Region: "us-east-2",
		Bucket: "mybucket",
		Path:   "mypath.csv",
	}

	require.Equal(
		t,
		"SELECT aws_s3.table_import_from_s3('tmp_users', '', '(format csv)', 'us-east-2', 'mybucket', 'mypath.csv');",
		prepareTable().UploadFileQuery(&file),
	)
}

func TestDropTargetTableQuery(t *testing.T) {
	require.Equal(
		t,
		"DROP TABLE IF EXISTS target_users;",
		prepareTable().DropTargetTableQuery(),
	)
}

func TestCreateTargetTableQuery(t *testing.T) {
	require.Equal(
		t,
		"CREATE UNLOGGED TABLE IF NOT EXISTS target_users (name::text, email::text);",
		prepareTable().CreateTargetTableQuery(),
	)
}

func TestCreateTargetIndexQuery(t *testing.T) {
	require.Equal(
		t,
		"CREATE UNIQUE INDEX BY target_users (email, name);",
		prepareTable().CreateTargetIndexQuery(),
	)
}

func TestPopulateTargetTableQuery(t *testing.T) {
	require.Equal(
		t,
		"WITH inserted_rows AS (INSERT INTO target_users (name, email) SELECT name::text, email::text FROM tmp_users ON CONFLICT DO NOTHING RETURNING 1) SELECT COUNT(1) AS count FROM inserted_rows;",
		prepareTable().PopulateTargetTableQuery(),
	)
}

func TestSwitchTablesQuery(t *testing.T) {
	require.Equal(
		t,
		`BEGIN;
		DROP TABLE IF EXISTS users;
		ALTER TABLE target_users RENAME TO users;
		COMMIT;`,
		prepareTable().SwitchTablesQuery(),
	)
}
