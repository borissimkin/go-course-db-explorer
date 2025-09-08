package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type Pagination struct {
	Offset int
	Limit  int
}

type Route struct {
	Method  string
	Pattern *regexp.Regexp
	Handler http.HandlerFunc
}

type Router struct {
	routes []Route
}

type Response struct {
	Response any `json:"response"`
}

type GetTableNamesResponse struct {
	Tables []string `json:"tables"`
}

type GetTableItemsResponse struct {
	Records []map[string]any `json:"records"`
}

type GetTableItemResponse struct {
	Record map[string]any `json:"record"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func NewErrorResponse(err error) []byte {
	resp := ErrorResponse{
		Error: err.Error(),
	}

	data, _ := json.Marshal(resp)
	return data
}

func NewRouter() *Router {
	return &Router{
		routes: make([]Route, 0),
	}
}

func (r *Router) Handle(method string, pattern string, handler http.HandlerFunc) {
	re := regexp.MustCompile("^" + pattern + "$")
	r.routes = append(r.routes, Route{Method: method, Pattern: re, Handler: handler})
}

type DbExplorer struct {
	DB         *sql.DB
	TableNames []string
	Columns    map[string][]string
	router     *Router
}

func (exp DbExplorer) getTableColumns(table string) ([]string, error) {
	columns := make([]string, 0)

	rows, err := exp.DB.Query("SHOW FULL COLUMNS FROM ?", table)
	if err != nil {
		return columns, nil
	}

	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return columns, err
		}

		columns = append(columns, name)
	}

	return columns, nil
}

func (exp DbExplorer) initColumns() error {
	for _, table := range exp.TableNames {
		columns, err := exp.getTableColumns(table)
		if err != nil {
			return err
		}

		exp.Columns[table] = columns
	}

	return nil
}

func isStringType(columnType string) bool {
	types := []string{
		"VARCHAR",
		"TEXT",
		"NVARCHAR",
	}

	for _, v := range types {
		if v == columnType {
			return true
		}
	}

	return false
}

func (exp DbExplorer) getTableItems(table string, pagination Pagination) ([]map[string]any, error) {
	res := make([]map[string]any, 0)

	rows, err := exp.DB.Query(fmt.Sprintf("SELECT * FROM %s LIMIT ? OFFSET ?", table), pagination.Limit, pagination.Offset)
	if err != nil {
		return res, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return res, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return res, err
	}

	for rows.Next() {
		values := make([]any, len(columns))
		for i := range values {
			if isStringType(columnTypes[i].DatabaseTypeName()) {
				values[i] = new(sql.NullString)
			} else {
				values[i] = new(any)
			}
		}

		if err := rows.Scan(values...); err != nil {
			return res, err
		}

		item := make(map[string]any)
		for i, v := range values {
			strOrNil, ok := v.(*sql.NullString)
			if ok {
				if strOrNil.Valid {
					item[columns[i]] = strOrNil.String
				} else {
					item[columns[i]] = nil
				}
			} else {
				item[columns[i]] = v
			}
		}

		res = append(res, item)
	}

	return res, nil
}

func (exp DbExplorer) getTableNames() ([]string, error) {
	tableNames := make([]string, 0)

	rows, err := exp.DB.Query("SHOW TABLES")
	if err != nil {
		return tableNames, nil
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
		DB:      db,
		router:  NewRouter(),
		Columns: make(map[string][]string),
	}

	tableNames, err := explorer.getTableNames()
	if err != nil {
		return explorer, err
	}

	explorer.TableNames = tableNames

	err = explorer.initColumns()
	if err != nil {
		return explorer, err
	}

	explorer.initRoutes()

	return explorer, nil
}

func (exp DbExplorer) initRoutes() {
	exp.router.Handle(http.MethodGet, "/", exp.handlerGetTableNames)
	exp.router.Handle(http.MethodGet, `/\w*`, exp.handlerGetTableItems)
	exp.router.Handle(http.MethodGet, `/\w*/[0-9]*`, exp.handlerGetTableItem)
}

func (exp DbExplorer) handlerGetTableNames(w http.ResponseWriter, r *http.Request) {
	tableResponse := GetTableNamesResponse{
		Tables: exp.TableNames,
	}

	response := Response{
		Response: tableResponse,
	}

	data, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func (exp DbExplorer) isValidTableName(tableName string) bool {
	for _, name := range exp.TableNames {
		if name == tableName {
			return true
		}
	}

	return false
}

func getQueryIntValue(query url.Values, key string, defaultValue int) int {
	if !query.Has(key) {
		return defaultValue
	}

	value := query.Get(key)

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return intValue
}

func getPagination(query url.Values) Pagination {
	return Pagination{
		Limit:  getQueryIntValue(query, "limit", 5),
		Offset: getQueryIntValue(query, "offset", 0),
	}
}

func (exp DbExplorer) getTableName(url string) (string, error) {
	tableName := strings.Split(url, "/")[1]
	if !exp.isValidTableName(tableName) {
		return "", fmt.Errorf("unknown table")
	}

	return tableName, nil
}

// todo: нужна проверка на инт?
func (exp DbExplorer) getId(url string) (string, error) {
	id := strings.Split(url, "/")[2]

	return id, nil
}

func (exp DbExplorer) handlerGetTableItems(w http.ResponseWriter, r *http.Request) {
	tableName, err := exp.getTableName(r.URL.Path)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write(NewErrorResponse(err))
		return
	}

	pagination := getPagination(r.URL.Query())

	items, err := exp.getTableItems(tableName, pagination)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	itemsResp := GetTableItemsResponse{
		Records: items,
	}

	resp := Response{
		Response: itemsResp,
	}
	data, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func (exp DbExplorer) getItem(table string, id string) (map[string]any, error) {
	res := make(map[string]any)

	row := exp.DB.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE id = ?", table), id)
	if row.Err() != nil {
		return res, row.Err()
	}

	rows, err := exp.DB.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", table))
	if err != nil {
		return res, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return res, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return res, err
	}

	values := make([]any, len(columns))
	for i := range values {
		if isStringType(columnTypes[i].DatabaseTypeName()) {
			values[i] = new(sql.NullString)
		} else {
			values[i] = new(any)
		}
	}

	err = row.Scan(values...)
	if err != nil {
		return res, err
	}

	for i, v := range values {
		strOrNil, ok := v.(*sql.NullString)
		if ok {
			if strOrNil.Valid {
				res[columns[i]] = strOrNil.String
			} else {
				res[columns[i]] = nil
			}
		} else {
			res[columns[i]] = v
		}
	}

	return res, nil
}

func (exp DbExplorer) handlerGetTableItem(w http.ResponseWriter, r *http.Request) {
	tableName, err := exp.getTableName(r.URL.Path)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write(NewErrorResponse(err))
		return
	}

	id, _ := exp.getId(r.URL.Path)

	item, err := exp.getItem(tableName, id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write(NewErrorResponse(fmt.Errorf("record not found")))
		return
	}

	res := GetTableItemResponse{
		Record: item,
	}

	resp := Response{
		Response: res,
	}

	data, _ := json.Marshal(resp)
	w.Write(data)
}

func (exp DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, route := range exp.router.routes {
		if route.Method != r.Method {
			continue
		}

		if route.Pattern.MatchString(r.URL.Path) {
			route.Handler(w, r)
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)

}
