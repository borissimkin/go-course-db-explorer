package main

import (
	"database/sql"
	"net/http"
)

type DbExplorer struct{}

func NewDbExplorer(db *sql.DB) (DbExplorer, error) {
	return DbExplorer{}, nil
}

func (exp DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
