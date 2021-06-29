package main

import (
	"database/sql"
	"time"
)

type StepInfo struct {
	Name     string
	Success  bool
	Duration int
}

type Results struct {
	ItemsRead  string
	ItemsTotal string
}

type Task struct {
	Database         Database
	Table            Table
	File             ImportedFile
	Results          Results
	Info             []StepInfo
	Errors           []error
	statementTimeout string
	containHeaders   bool
}

// Caller of steps
func (task *Task) Run() {
	steps := [](func() (string, error)){
		task.reset,
		task.setStatementTimeout,
		task.dropTemporaryTable,
		task.createTemporaryTable,
		task.populateTemporaryTable,
		task.createTargetTable,
		task.createTargetIndex,
		task.populateTargetTable,
		task.analyzeTargetTable,
		task.switchTables,
	}
	for i := range steps {
		task.runStep(steps[i])
	}
	task.Database.Close()
}

func (task *Task) IsSuccess() bool {
	return (len(task.Errors) == 0)
}

// Definitions of individual steps

func (task *Task) reset() (name string, err error) {
	name = "reset"

	task.Info = []StepInfo{}
	task.Errors = []error{}
	task.Results = Results{}
	task.containHeaders = false

	return
}

func (task *Task) setStatementTimeout() (name string, err error) {
	name = "set_statement_timeout"
	if task.statementTimeout != "0min" {
		_, err = task.query("SET statement_timeout = '%s';", task.statementTimeout)
	}
	return
}

func (task *Task) dropTemporaryTable() (name string, err error) {
	name = "drop_temporary_table"
	_, err = task.query(task.Table.DropTemporaryTableQuery())
	return
}

func (task *Task) createTemporaryTable() (name string, err error) {
	name = "create_temporary_table"
	_, err = task.query(task.Table.CreateTemporaryTableQuery())
	return
}

func (task *Task) populateTemporaryTable() (name string, err error) {
	name = "populate_temporary_table"
	rows := &sql.Rows{}
	rows, err = task.query(task.Table.UploadFileQuery(task.File))
	if rows != nil {
		err = rows.Scan(&(task.Results.ItemsRead))
	}
	return
}

func (task *Task) createTargetTable() (name string, err error) {
	name = "create_target_table"
	_, err = task.query(task.Table.CreateTargetTableQuery())
	return
}

func (task *Task) createTargetIndex() (name string, err error) {
	name = "create_target_index"
	_, err = task.query(task.Table.CreateTargetIndexQuery())
	return
}

func (task *Task) populateTargetTable() (name string, err error) {
	name = "populate_target_table"
	rows := &sql.Rows{}
	if rows, err = task.query(task.Table.PopulateTargetTableQuery()); rows != nil {
		err = rows.Scan(&(task.Results.ItemsTotal))
	}
	return
}

func (task *Task) analyzeTargetTable() (name string, err error) {
	name = "analyze_target_table"
	_, err = task.query(task.Table.AnalyzeTargetTableQuery())
	return
}

func (task *Task) switchTables() (name string, err error) {
	name = "switch_tables"
	_, err = task.query(task.Table.SwitchTablesQuery())
	return
}

// Helpers

func (task *Task) runStep(f func() (string, error)) {
	startTime := time.Now()
	success := task.IsSuccess()
	if success == false {
		return
	}

	name, err := f()
	if err != nil {
		task.Errors = append(task.Errors, err)
		success = false
	}

	stepInfo := StepInfo{
		Name:     name,
		Duration: int(time.Now().Sub(startTime) / time.Microsecond),
		Success:  success,
	}
	task.Info = append(task.Info, stepInfo)
}

func (task *Task) query(q string, args ...interface{}) (*sql.Rows, error) {
	return task.Database.Query(q, args)
}
