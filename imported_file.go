package main

import (
	"errors"
	"fmt"
	"strings"
)

type ImportedFile interface {
	Validate() error
	UploadQuery(string) string
}

// Implementation of the imported file from the local filesystem
type LocalFile struct {
	Path string
}

func (file *LocalFile) Validate() error {
	if file.Path == "" {
		return errors.New("The path to the local file should be present")
	}
	if strings.Contains(file.Path, "'") {
		return errors.New("The path to the local file should NOT contain single quotes")
	}
	return nil
}

func (file *LocalFile) UploadQuery(table string) string {
	return fmt.Sprintf("COPY %s FROM %s WITH CSV;", table, file.Path)
}

// Implementation of the imported file from an AWS S3 bucket
type S3File struct {
	Region string
	Bucket string
	Path   string
}

func (file *S3File) Validate() error {
	if file.Region == "" {
		return errors.New("The AWS S3 region should be present")
	}
	if file.Bucket == "" {
		return errors.New("The bucket of the S3 file should be present")
	}
	if file.Path == "" {
		return errors.New("The path to the file in the AWS S3 bucket should be present")
	}

	if strings.Contains(file.Region, "'") {
		return errors.New("The AWS S3 region should NOT contain single quotes")
	}
	if strings.Contains(file.Bucket, "'") {
		return errors.New("The AWS S3 bucket should NOT contain single quotes")
	}
	if strings.Contains(file.Path, "'") {
		return errors.New("The path to the file in AWS S3 bucket should NOT contain single quotes")
	}
	return nil
}

func (file *S3File) UploadQuery(table string) string {
	return fmt.Sprintf(
		"SELECT aws_s3.table_import_from_s3('%s', '', '(format csv)', '%s', '%s', '%s');",
		table,
		file.Region,
		file.Bucket,
		file.Path,
	)
}

// Implementation of the imported file as in-memory list of lines
type InMemoryFile struct {
	Lines [][]string
}

func (file *InMemoryFile) Validate() error {
	if len(file.Lines) < 2 {
		return errors.New("There's nothing to import")
	}
	width := len(file.Lines[0])
	if width == 0 {
		return errors.New("There's nothing to import")
	}
	for _, item := range file.Lines[0] {
		if item == "" {
			return errors.New("Headers should not be blank")
		}
	}
	for i, line := range file.Lines {
		if len(line) != width {
			msg := fmt.Sprintf("Unexpected length of line %d (expected %d, actual %d)", i, width, len(line))
			return errors.New(msg)
		}
		for j, item := range line {
			if strings.Contains(item, "'") {
				msg := fmt.Sprintf("The line %d (item %d) should NOT contain single quotes", i, j)
				return errors.New(msg)
			}
		}
	}
	return nil
}

func (file *InMemoryFile) UploadQuery(table string) string {
	headers := []string{}
	for _, item := range file.Lines[0] {
		headers = append(headers, fmt.Sprintf("%s", item))
	}
	values := []string{}
	for _, line := range file.Lines[1:] {
		items := []string{}
		for _, item := range line {
			items = append(items, fmt.Sprintf("'%s'", item))
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(items, ", ")))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table, strings.Join(headers, ", "), strings.Join(values, ", "))
}
