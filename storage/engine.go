package storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Engine struct {
	nodeID string
	db     *sql.DB
}

func NewEngine(nodeID string) *Engine {
	// Connect to MySQL server on localhost without selecting a specific database.
	// The user is "root" and password is "root"
	dsn := "root:root@tcp(localhost:3306)/?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	// Verify the connection
	if err := db.Ping(); err != nil {
		log.Printf("Warning: Could not ping MySQL server: %v", err)
	}

	return &Engine{
		nodeID: nodeID,
		db:     db,
	}
}

func (e *Engine) getDBName(dbName string) string {
	// We prefix the DB name with the node ID so that each node
	// stores its data independently on the same MySQL server.
	// e.g. "node1_mydb", "node2_mydb"
	return fmt.Sprintf("node%s_%s", e.nodeID, dbName)
}

func (e *Engine) CreateDB(dbName string) error {
	name := e.getDBName(dbName)
	_, err := e.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", name))
	return err
}

func (e *Engine) DropDB(dbName string) error {
	name := e.getDBName(dbName)
	_, err := e.db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
	return err
}

func (e *Engine) CreateTable(dbName, tableName string, attributes []string) error {
	name := e.getDBName(dbName)

	// Construct column definitions, using VARCHAR(255) for all columns as a generic type
	cols := []string{}
	for _, attr := range attributes {
		cols = append(cols, fmt.Sprintf("`%s` VARCHAR(255)", attr))
	}

	if len(cols) == 0 {
		cols = append(cols, "`id` INT AUTO_INCREMENT PRIMARY KEY") // Fallback
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)", name, tableName, strings.Join(cols, ", "))
	_, err := e.db.Exec(query)
	return err
}

func (e *Engine) DropTable(dbName, tableName string) error {
	name := e.getDBName(dbName)
	_, err := e.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", name, tableName))
	return err
}

func (e *Engine) Insert(dbName, tableName string, record map[string]interface{}) error {
	name := e.getDBName(dbName)

	cols := []string{}
	placeholders := []string{}
	args := []interface{}{}

	for k, v := range record {
		cols = append(cols, fmt.Sprintf("`%s`", k))
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}

	query := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		name, tableName, strings.Join(cols, ", "), strings.Join(placeholders, ", "))

	_, err := e.db.Exec(query, args...)
	return err
}

func buildWhereClause(query map[string]interface{}) (string, []interface{}) {
	if len(query) == 0 {
		return "", nil
	}
	conditions := []string{}
	args := []interface{}{}
	for k, v := range query {
		conditions = append(conditions, fmt.Sprintf("`%s` = ?", k))
		args = append(args, v)
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func (e *Engine) Select(dbName, tableName string, query map[string]interface{}) ([]map[string]interface{}, error) {
	name := e.getDBName(dbName)

	whereClause, args := buildWhereClause(query)
	sqlQuery := fmt.Sprintf("SELECT * FROM `%s`.`%s`%s", name, tableName, whereClause)

	rows, err := e.db.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		m := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			if *val != nil {
				// Convert byte arrays to strings (MySQL driver returns VARCHAR as []byte)
				if b, ok := (*val).([]byte); ok {
					m[colName] = string(b)
				} else {
					m[colName] = *val
				}
			} else {
				m[colName] = nil
			}
		}
		results = append(results, m)
	}
	return results, nil
}

func (e *Engine) Update(dbName, tableName string, query, update map[string]interface{}) (int, error) {
	name := e.getDBName(dbName)

	setClauses := []string{}
	args := []interface{}{}

	for k, v := range update {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", k))
		args = append(args, v)
	}

	if len(setClauses) == 0 {
		return 0, nil
	}

	whereClause, whereArgs := buildWhereClause(query)
	args = append(args, whereArgs...)

	sqlQuery := fmt.Sprintf("UPDATE `%s`.`%s` SET %s%s", name, tableName, strings.Join(setClauses, ", "), whereClause)

	res, err := e.db.Exec(sqlQuery, args...)
	if err != nil {
		return 0, err
	}

	count, _ := res.RowsAffected()
	return int(count), nil
}

func (e *Engine) Delete(dbName, tableName string, query map[string]interface{}) (int, error) {
	name := e.getDBName(dbName)

	whereClause, args := buildWhereClause(query)
	sqlQuery := fmt.Sprintf("DELETE FROM `%s`.`%s`%s", name, tableName, whereClause)

	res, err := e.db.Exec(sqlQuery, args...)
	if err != nil {
		return 0, err
	}

	count, _ := res.RowsAffected()
	return int(count), nil
}

func (e *Engine) ExecRaw(dbName, rawSQL string) ([]map[string]interface{}, error) {
	name := e.getDBName(dbName)
	// Open a connection specifically to this node's database
	dsn := fmt.Sprintf("root:root@tcp(localhost:3306)/%s?parseTime=true", name)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	sqlUpper := strings.ToUpper(strings.TrimSpace(rawSQL))
	isSelect := strings.HasPrefix(sqlUpper, "SELECT") || strings.HasPrefix(sqlUpper, "SHOW")

	if isSelect {
		rows, err := db.Query(rawSQL)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		var results []map[string]interface{}
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			if err := rows.Scan(columnPointers...); err != nil {
				return nil, err
			}

			m := make(map[string]interface{})
			for i, colName := range cols {
				val := columnPointers[i].(*interface{})
				if *val != nil {
					if b, ok := (*val).([]byte); ok {
						m[colName] = string(b)
					} else {
						m[colName] = *val
					}
				} else {
					m[colName] = nil
				}
			}
			results = append(results, m)
		}
		return results, nil
	} else {
		res, err := db.Exec(rawSQL)
		if err != nil {
			return nil, err
		}
		count, _ := res.RowsAffected()
		return []map[string]interface{}{{"rows_affected": count}}, nil
	}
}
