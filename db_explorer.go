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

type ValidationError struct {
	Field string
}

func NewValidationError(field string) ValidationError {
	return ValidationError{
		Field: field,
	}
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("field %s have invalid type", e.Field)
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

type DeleteTableItemResponse struct {
	Deleted int `json:"deleted"`
}

type UpdateTableItemResponse struct {
	Updated int `json:"updated"`
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
	DB           *sql.DB
	TableNames   []string
	TableColumns map[string][]*sql.ColumnType
	router       *Router
}

type ValidationOptions struct {
	IgnorePk               bool
	IgnoreNotProvidedField bool
}

func isNumberType(columnType string) bool {
	types := []string{
		"NUMBER",
	}

	for _, v := range types {
		if v == columnType {
			return true
		}
	}

	return false
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

func (exp DbExplorer) initTableColumns() {
	for _, table := range exp.TableNames {
		columns, err := exp.getColumnTypes(table)
		if err != nil {
			panic(err)
		}

		exp.TableColumns[table] = columns
	}
}

func NewDbExplorer(db *sql.DB) (DbExplorer, error) {
	explorer := DbExplorer{
		DB:           db,
		router:       NewRouter(),
		TableColumns: make(map[string][]*sql.ColumnType),
	}

	tableNames, err := explorer.getTableNames()
	if err != nil {
		return explorer, err
	}

	explorer.TableNames = tableNames

	explorer.initTableColumns()
	explorer.initRoutes()

	return explorer, nil
}

func (exp DbExplorer) initRoutes() {
	exp.router.Handle(http.MethodGet, "/", exp.handlerGetTableNames)
	exp.router.Handle(http.MethodGet, `/\w*`, exp.handlerGetTableItems)
	exp.router.Handle(http.MethodGet, `/\w*/[0-9]*`, exp.handlerGetTableItem)
	exp.router.Handle(http.MethodPut, `/\w*/`, exp.handlerCreateItem)
	exp.router.Handle(http.MethodDelete, `/\w*/[0-9]*`, exp.handlerDeleteItem)
	exp.router.Handle(http.MethodPost, `/\w*/[0-9]*`, exp.handlerUpdateItem)
}

func (exp DbExplorer) updateItem(table string, form map[string]any, columns []*sql.ColumnType, primaryKey string, pkValue any) (pk int64, err error) {
	// todo: вынести
	// columnNames := make([]string, 0)
	// for _, c := range columns {
	// 	name := c.Name()
	// 	if name == primaryKey {
	// 		continue
	// 	}

	// 	columnNames = append(columnNames, name)
	// }

	// setColumnsQuery := make([]string, len(columnNames))
	// for i, c := range columnNames {
	// 	setColumnsQuery[i] = fmt.Sprintf("%s = ?", c)
	// }

	// setColumnsQueryJoined := strings.Join(setColumnsQuery, ", ")

	// values := make([]any, len(columnNames))
	// // todo: убрать проверку на nil и из create
	// for i, c := range columnNames {
	// 	values[i] = form[c]
	// }

	columnNames := make([]string, 0)
	values := make([]any, 0)
	for k, v := range form {
		columnNames = append(columnNames, k)
		values = append(values, v)
	}

	setColumnsQuery := make([]string, len(columnNames))
	for i, c := range columnNames {
		setColumnsQuery[i] = fmt.Sprintf("%s = ?", c)
	}

	setColumnsQueryJoined := strings.Join(setColumnsQuery, ", ")

	args := make([]any, 0)
	args = append(args, values...)

	args = append(args, pkValue)

	// todo "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '(description = ?) WHERE id = ?' at line 1"

	result, err := exp.DB.Exec(fmt.Sprintf("UPDATE %s SET (%s) WHERE %s = ?", table, setColumnsQueryJoined, primaryKey), args...)
	if err != nil {
		return 0, err
	}

	pk, err = result.RowsAffected()

	return pk, err
}

func (exp DbExplorer) processForm(form map[string]any, columns []*sql.ColumnType, primaryKey string, validationOptions ValidationOptions) error {
	for _, c := range columns {
		name := c.Name()
		value, has := form[name]
		nullable, ok := c.Nullable()
		if !ok {
			return fmt.Errorf("db driver does not support nullable")
		}

		if name == primaryKey {
			if has && !validationOptions.IgnorePk {
				return NewValidationError(name)
			}
			continue
		}

		if has {
			switch value.(type) {
			case float64:
				if !isNumberType(c.DatabaseTypeName()) {
					return NewValidationError(name)
				}

			case string:
				if !isStringType(c.DatabaseTypeName()) {
					return NewValidationError(name)
				}
			case nil:
				if !nullable {
					return NewValidationError(name)
				}
			}

			form[name] = value
			continue
		}

		if validationOptions.IgnoreNotProvidedField {
			continue
		}

		if !nullable {
			return NewValidationError(name)
		}

		form[name] = nil
	}

	return nil
}

func (exp DbExplorer) handlerUpdateItem(w http.ResponseWriter, r *http.Request) {
	tableName, err := exp.getTableName(r.URL.Path)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write(NewErrorResponse(err))
		return
	}

	columns, err := exp.getColumnTypesFromCache(tableName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	primaryKey, err := exp.getPrimaryKey(tableName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	form := make(map[string]any)
	err = json.NewDecoder(r.Body).Decode(&form)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := exp.getId(r.URL.Path)

	err = exp.processForm(form, columns, primaryKey, ValidationOptions{
		IgnorePk:               false,
		IgnoreNotProvidedField: true,
	})

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(NewErrorResponse(err))
		return
	}

	pk, err := exp.updateItem(tableName, form, columns, primaryKey, id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	updated := 0
	if pk > 0 {
		updated = 1
	}

	result := UpdateTableItemResponse{
		Updated: updated,
	}
	response := Response{
		Response: result,
	}

	data, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func (exp DbExplorer) handlerDeleteItem(w http.ResponseWriter, r *http.Request) {
	tableName, err := exp.getTableName(r.URL.Path)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write(NewErrorResponse(err))
		return
	}

	id := exp.getId(r.URL.Path)

	pkName, err := exp.getPrimaryKey(tableName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	pk, err := exp.deleteItem(tableName, pkName, id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	deleted := 0
	if pk > 0 {
		deleted = 1
	}

	result := DeleteTableItemResponse{
		Deleted: deleted,
	}
	response := Response{
		Response: result,
	}

	data, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func (exp DbExplorer) deleteItem(table string, pkName string, pkValue any) (pk int64, err error) {
	result, err := exp.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s=?", table, pkName), pkValue)
	if err != nil {
		return pk, err
	}

	id, err := result.RowsAffected()
	if err != nil {
		return pk, err
	}

	return id, nil
}

func (exp DbExplorer) createItem(table string, form map[string]any, columns []*sql.ColumnType, primaryKey string) (pk any, err error) {
	columnNames := make([]string, 0)
	for _, c := range columns {
		name := c.Name()
		if name == primaryKey {
			continue
		}

		columnNames = append(columnNames, name)
	}

	columnNamesQuery := strings.Join(columnNames, ", ")

	values := make([]any, len(columnNames))
	for i, c := range columnNames {
		value, ok := form[c]
		if !ok {
			values[i] = nil
			continue
		}

		values[i] = value
	}

	valuesPlaceholder := make([]string, len(values))
	for i := range valuesPlaceholder {
		valuesPlaceholder[i] = "?"
	}

	queryValuePlaceholder := strings.Join(valuesPlaceholder, ", ")

	result, err := exp.DB.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columnNamesQuery, queryValuePlaceholder), values...)
	if err != nil {
		return 0, err
	}

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastInsertId, err
}

func (exp DbExplorer) getPrimaryKey(table string) (string, error) {
	rows, err := exp.DB.Query(fmt.Sprintf(`SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = '%v'
      AND CONSTRAINT_NAME = 'PRIMARY'
      AND TABLE_SCHEMA = DATABASE()`, table))
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var name string
	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return "", err
		}

	}

	return name, nil
}

func (exp DbExplorer) handlerCreateItem(w http.ResponseWriter, r *http.Request) {
	tableName, err := exp.getTableName(r.URL.Path)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write(NewErrorResponse(err))
		return
	}

	columns, err := exp.getColumnTypesFromCache(tableName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	primaryKey, err := exp.getPrimaryKey(tableName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	form := make(map[string]any)
	err = json.NewDecoder(r.Body).Decode(&form)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// todo: сделать функцию валидации исходя Rows

	for _, c := range columns {
		name := c.Name()
		if name == primaryKey {
			continue
		}

		value, has := form[name]
		if !has {
			form[name] = nil
			continue
		}

		form[name] = value
	}
	id, err := exp.createItem(tableName, form, columns, primaryKey)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	result := make(map[string]any)
	result[primaryKey] = id

	response := Response{
		Response: result,
	}

	data, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
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

func (exp DbExplorer) getId(url string) string {
	id := strings.Split(url, "/")[2]

	return id
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

func (exp DbExplorer) getColumnTypes(table string) ([]*sql.ColumnType, error) {
	res := make([]*sql.ColumnType, 0)

	rows, err := exp.DB.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", table))
	if err != nil {
		return res, err
	}

	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return res, err
	}

	return columnTypes, nil
}

func (exp DbExplorer) getColumnTypesFromCache(table string) ([]*sql.ColumnType, error) {
	columnTypes, ok := exp.TableColumns[table]
	if !ok {
		return columnTypes, fmt.Errorf("table=%s doesnt have cache", table)
	}

	return columnTypes, nil
}

func (exp DbExplorer) getItem(table string, id string) (map[string]any, error) {
	res := make(map[string]any)

	row := exp.DB.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE id = ?", table), id)
	if row.Err() != nil {
		return res, row.Err()
	}

	columnTypes, err := exp.getColumnTypesFromCache(table)
	if err != nil {
		return res, err
	}

	values := make([]any, len(columnTypes))
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
				res[columnTypes[i].Name()] = strOrNil.String
			} else {
				res[columnTypes[i].Name()] = nil
			}
		} else {
			res[columnTypes[i].Name()] = v
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

	id := exp.getId(r.URL.Path)

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
