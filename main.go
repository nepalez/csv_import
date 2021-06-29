package main

import "fmt"

func main() {
	task := Task{
		File: &S3File{
			Region: "us-east-3",
			Bucket: "foobar",
			Path:   "baz.csv",
		},
		Database: Database{
			Host:     "localhost",
			Port:     5432,
			Name:     "nepalez",
			User:     "test",
			Password: "test",
		},
		Table: Table{
			Name: "users",
			TypedColumns: []string{
				"name::text",
			},
			IndexBy: []string{
				"name",
			},
		},
	}

	task.Run()
	fmt.Printf("success: %v\nerrors: %v\n", task.IsSuccess(), task.Errors)
	fmt.Printf("foo")
}
