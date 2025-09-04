package main

import (
	"database/sql"
	"net/http"
)

type DbExplorer struct {
	DB         *sql.DB
	TableNames []string
}

func (exp DbExplorer) getTableNames() ([]string, error) {
	tableNames := make([]string, 0)

	rows, err := exp.DB.Query("SHOW TABLES")
	if err != nil {
		return []string{}, nil
	}

	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return tableNames, err
		}

		tableNames = append(tableNames, name)
	}

	return tableNames, nil
}

func NewDbExplorer(db *sql.DB) (DbExplorer, error) {
	explorer := DbExplorer{
		DB: db,
	}

	tableNames, err := explorer.getTableNames()
	if err != nil {
		return explorer, err
	}

	explorer.TableNames = tableNames

	return explorer, nil
}

func (exp DbExplorer) handlerGetTableNames(w http.ResponseWriter, r *http.Request) {

}

func (exp DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.HandleFunc("/", exp.handlerGetTableNames)
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
