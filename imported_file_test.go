package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// LocalFile

func TestLocalFileImplementImportedFileInterface(t *testing.T) {
	require.Implements(t, (*ImportedFile)(nil), new(LocalFile))
}

func TestLocalFileUploadQuery(t *testing.T) {
	file := LocalFile{Path: "/myfile.csv"}
	require.Equal(
		t,
		"COPY mytable FROM /myfile.csv WITH CSV;",
		file.UploadQuery("mytable"),
	)
}

func TestLocalFileValidateValid(t *testing.T) {
	file := LocalFile{Path: "/myfile.csv"}
	require.Nil(t, file.Validate())
}

func TestLocalFileValidateInvalid(t *testing.T) {
	files := []LocalFile{
		{Path: ""},
		{Path: "''myfile.csv"},
	}

	for _, file := range files {
		require.NotNil(t, file.Validate())
	}
}

// S3File

func TestS3FileImplementImportedFileInterface(t *testing.T) {
	require.Implements(t, (*ImportedFile)(nil), new(S3File))
}

func TestS3FileQuery(t *testing.T) {
	file := S3File{Region: "us-east-2", Bucket: "mybucket", Path: "/myfile.csv"}
	require.Equal(
		t,
		"SELECT aws_s3.table_import_from_s3('mytable', '', '(format csv)', 'us-east-2', 'mybucket', '/myfile.csv');",
		file.UploadQuery("mytable"),
	)
}

func TestS3FileValidateValid(t *testing.T) {
	file := S3File{Region: "us-east-2", Bucket: "mybucket", Path: "/myfile.csv"}
	require.Nil(t, file.Validate())
}

func TestS3FileValidateInvalid(t *testing.T) {
	files := []S3File{
		{Region: "", Bucket: "mybucket", Path: "/myfile.csv"},
		{Region: "us-east-2", Bucket: "", Path: "/myfile.csv"},
		{Region: "us-east-2", Bucket: "mybucket", Path: ""},
		{Region: "''us-east-2", Bucket: "mybucket", Path: "/myfile.csv"},
		{Region: "us-east-2", Bucket: "''mybucket", Path: "/myfile.csv"},
		{Region: "us-east-2", Bucket: "mybucket", Path: "''/myfile.csv"},
	}

	for _, file := range files {
		require.NotNil(t, file.Validate())
	}
}

// InMemoryFile

func TestInMemoryFileImplementImportedFileInterface(t *testing.T) {
	require.Implements(t, (*ImportedFile)(nil), new(InMemoryFile))
}

func TestInMemoryFileQuery(t *testing.T) {
	file := InMemoryFile{
		Lines: [][]string{
			{"name", "email"},
			{"Andy", "andy@ex.com"},
			{"July", ""},
		},
	}
	require.Equal(
		t,
		"INSERT INTO mytable (name, email) VALUES ('Andy', 'andy@ex.com'), ('July', '');",
		file.UploadQuery("mytable"),
	)
}

func TestInMemoryFileValidateValid(t *testing.T) {
	file := InMemoryFile{
		Lines: [][]string{
			{"name", "email"},
			{"Andy", "andy@ex.com"},
			{"July", ""},
		},
	}
	require.Nil(t, file.Validate())
}

func TestInMemoryFileValidateInvalid(t *testing.T) {
	files := []InMemoryFile{
		{Lines: [][]string{}},
		{Lines: [][]string{{}}},
		{Lines: [][]string{{"foo", ""}, {"1", "2"}}},
		{Lines: [][]string{{"foo", "''bar"}, {"1", "2"}}},
		{Lines: [][]string{{"foo", "bar"}, {"1"}}},
		{Lines: [][]string{{"foo", "bar"}, {"1", "''2"}}},
	}

	for _, file := range files {
		require.NotNil(t, file.Validate())
	}
}
