package postgres

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// Test is a test function that currently tests the InsertKeyValue,
// Delete, InsertJSON, and QueryKeyValue JSON. This should be moved
// to an actual test.
func Test() error {

	kv := KeyValue{
		Key:   "InsertJSON",
		Value: time.Now().UTC().String(),
	}

	kv.Key = "kv"
	err := InsertKeyValue("", kv.Key, kv.Value)
	if err != nil {
		return err
	}

	kv.Key = "delete"
	err = InsertKeyValue("", kv.Key, kv.Value)
	if err != nil {
		return err
	}
	err = Delete("", kv.Key)
	if err != nil {
		return err
	}

	kv.Key = "kvjson"
	err = InsertJSON("", kv.Key, kv)
	if err != nil {
		return err
	}

	kv.Key = "kvjsonb"
	err = InsertJSON("kvjsonb", kv.Key, kv)
	if err != nil {
		return err
	}

	err = QueryKeyValue("")
	if err != nil {
		return err
	}

	err = QueryKeyValue("select key, value from kvjson")
	if err != nil {
		return err
	}

	err = QueryKeyValue("select key, value from kvjsonb")
	if err != nil {
		return err
	}

	return nil
}

// InsertJSON inserts JSON into the table (default: kvjson) by
// marshalling an interface. The main benefit here is that we
// validate that we are inserting valid JSON by marshalling prior
// to insertion.
func InsertJSON(table, key string, value interface{}) error {
	if table == "" {
		table = "kvjson"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = db.Exec("insert into "+table+" values ($1, $2);", key, b)
	if err != nil {
		return err
	}

	return nil
}

// InsertKeyValue inserts a key/value pair into a table. The name
// of the table defaults to kv.
func InsertKeyValue(table, key, value string) error {
	if table == "" {
		table = "kv"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("insert into "+table+" values ($1, $2);", key, value)
	if err != nil {
		return err
	}

	return nil
}

// Exec excutes a command and returns the rows affected, or an error.
// This is primarily to show RowsAffected() which we have ommitted
// in other places where Exec is used.
func Exec(sql string, args ...interface{}) (int64, error) {

	db, err := DbFromEnv()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	result, err := db.Exec(sql, args)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, err
}

// Delete deletes from table where the key column matches the
// key parameter.
func Delete(table, key string) error {
	if table == "" {
		table = "kv"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("delete from "+table+" where key = $1;", key)
	if err != nil {
		return err
	}

	return nil
}

// QueryKeyValue is a function that is designed to return a Key/Value
// pair which we marshal to JSON and write to the standard output.
// It is partially designed to be an example, and to guarantee output
// shape when we pair with InsertKeyValue. We also default the query to:
// select key, value from kv
func QueryKeyValue(query string) error {
	if query == "" {
		query = "select key, value from kv"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		k := KeyValue{}
		rows.Scan(&k.Key, &k.Value)
		b, err := json.Marshal(&k)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// QueryString performs a query (with a default) which returns a
// single string, which we then print to the standard output.
// This function is designed for queries that have a single return
// value (e.g. a json/jsonb column)
func QueryString(query string) error {
	if query == "" {
		query = "select value from kv"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	b := ""
	for rows.Next() {
		err := rows.Scan(&b)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// QueryJSON selects performs a select from the database (with a default)
// and builds a JSON map which we write to the standard output.
func QueryJSON(query string) error {
	// TODO: we could move this to the cli command and potentially
	// return an error on an empty string here.
	if query == "" {
		query = "select value from kv"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	values := make([]interface{}, numColumns)
	for i := range values {
		values[i] = new(interface{})
	}

	dest := make(map[string]interface{}, numColumns)
	for i, column := range columns {
		dest[column] = *(values[i].(*interface{}))
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return err
		}
		dest := make(map[string]interface{}, numColumns)
		for i, column := range columns {
			dest[column] = *(values[i].(*interface{}))
		}

		b, err := json.Marshal(dest)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// ListTables selects all tables from the current database and outputs them
// in JSON format.
func ListTables() error {
	query := "select tablename from pg_tables where schemaname = 'public';"
	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	result := struct {
		Name string `json:"name"`
	}{}
	for rows.Next() {
		rows.Scan(&result.Name)
		b, err := json.Marshal(&result)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// CreateTable creates a table in the database with columns key of type varchar(256)
// and value of type valueType which defaults to jsonb.
func CreateTable(name, valueType string) error {

	if valueType == "" {
		valueType = "jsonb"
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	sql1 := `
	create table %s (
		key varchar(256),
		value %s
	);
	`

	_, err = db.Exec(fmt.Sprintf(sql1, name, valueType))
	if err != nil {
		return err
	}

	return nil
}

// DeleteTable deletes a table from the database
func DeleteTable(name string) error {
	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	sql1 := `drop table %s;`

	_, err = db.Exec(fmt.Sprintf(sql1, name))
	if err != nil {
		return err
	}

	return nil
}

// InsertStdinBulk takes one or more records from the standard input and inserts
// them individually using insertBulkJSON rather than InsertStdin's insertJSON.
// This uses the bulk import approach outlined in the pq docs:
// https://pkg.go.dev/github.com/lib/pq#hdr-Bulk_imports
// We set a batchSize, which defaults to 100 if batchSize == 0
func InsertStdinBulk(table string, batchSize int) error {
	if batchSize == 0 {
		batchSize = 100
	}

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	type keyValue struct {
		Key   string
		Value interface{}
	}

	insertBulkJSON := func(table string, values []keyValue) (int64, error) {

		txn, err := db.Begin()
		if err != nil {
			return 0, err
		}

		stmt, err := txn.Prepare(pq.CopyIn(table, "key", "value"))
		if err != nil {
			return 0, err
		}

		for _, value := range values {
			b, err := json.Marshal(value.Value)
			if err != nil {
				return 0, err
			}

			_, err = stmt.Exec(value.Key, string(b))
			if err != nil {
				return 0, err
			}
		}

		result, err := stmt.Exec()
		if err != nil {
			return 0, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}

		err = stmt.Close()
		if err != nil {
			return 0, err
		}

		err = txn.Commit()
		if err != nil {
			return 0, err
		}

		return rowsAffected, nil
	}

	scanner := bufio.NewScanner(os.Stdin)

	map1 := map[string]interface{}{}

	batch := []keyValue{}
	i := 0
	for scanner.Scan() {

		err := json.Unmarshal(scanner.Bytes(), &map1)
		if err != nil {
			return nil
		}
		key := ""
		if val, ok := map1["Key"]; ok {
			if k, ok := val.(string); ok {
				key = k
			}
		}
		if key == "" {
			key = uuid.NewString()
		}
		kv := keyValue{
			Key:   key,
			Value: map1,
		}
		batch = append(batch, kv)

		i++
		if batchSize == i {
			rowsAffected, err := insertBulkJSON(table, batch)
			if err != nil {
				return err
			}
			log.Printf("Rows Affected: %d\n", rowsAffected)
			batch = []keyValue{}
		}
	}
	if len(batch) > 0 {
		rowsAffected, err := insertBulkJSON(table, batch)
		if err != nil {
			return err
		}
		log.Printf("Rows Affected: %d\n", rowsAffected)
		batch = []keyValue{}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// InsertStdin takes one or more records from the standard input and inserts
// them individually using insertJSON which is similar to InsertJSON but reuses
// the database connection so we avoid exhausting them in a loop.
func InsertStdin(table string) error {

	db, err := DbFromEnv()
	if err != nil {
		return err
	}
	defer db.Close()

	insertJSON := func(table, key string, value interface{}) error {
		b, err := json.Marshal(value)
		if err != nil {
			return err
		}

		_, err = db.Exec("insert into "+table+" values ($1, $2);", key, b)
		if err != nil {
			return err
		}

		return nil
	}

	scanner := bufio.NewScanner(os.Stdin)

	map1 := map[string]interface{}{}
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &map1)
		if err != nil {
			return nil
		}
		key := ""
		if val, ok := map1["Key"]; ok {
			if k, ok := val.(string); ok {
				key = k
			}
		}
		if key == "" {
			key = uuid.NewString()
		}

		err = insertJSON(table, key, map1)
		if err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// DbFromEnv creates a *sql.DB authenticated by the environment variable
// POSTGRES_SQL
func DbFromEnv() (*sql.DB, error) {
	connStr := os.Getenv("POSTGRES_URL")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// KeyValue is a simple struct to represent a Key/Value pair
type KeyValue struct {
	Key   string
	Value string
}
