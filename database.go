package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type Database struct {
	Host     string
	Port     int
	Name     string
	User     string
	Password string
	pool     *sql.DB
}

func (db *Database) dsn() (string, error) {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		db.Host,
		db.Port,
		db.User,
		db.Password,
		db.Name,
	), nil
}

func (db *Database) Open() (err error) {
	if db.pool != nil {
		var dsn string
		if dsn, err = db.dsn(); err == nil {
			db.pool, err = sql.Open("postgres", dsn)
		}
	}

	return
}

func (db *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if err := db.Open(); err != nil {
		return &sql.Rows{}, err
	}

	return db.pool.Query(query, args)
}

func (db *Database) Close() (err error) {
	if db.pool != nil {
		err = db.pool.Close()
	}

	return
}
