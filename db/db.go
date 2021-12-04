// Package db управляет подключением и записью данных в БД.
package db

import (
	"database/sql"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
	"log"
	"mqtt2clickhouse/message"
	"strings"
	"sync"
)

// tablesInfo схема таблиц БД.
type tablesInfo map[string][]message.ColumnsType

// ExplorerDB хранит подключение к БД и схему ее таблиц.
type ExplorerDB struct {
	connect      *sql.DB
	tablesFromDB *tablesInfo
	mu           sync.RWMutex
}

// Connect выполняет подключение к базе данных.
func (e *ExplorerDB) Connect(dataSource string) error {
	errMessage := "Не удалось подключится к базе %s по причине %s\n"
	var err error

	e.connect, err = sql.Open("clickhouse", dataSource)
	if err != nil {
		return fmt.Errorf(errMessage, dataSource, err)
	}
	if err := e.connect.Ping(); err != nil {
		return fmt.Errorf(errMessage, dataSource, err)
	}

	log.Printf("Подключение к базе %s успешно завершено.\n", dataSource)
	return nil
}

// CloseConnect закрывает соединение с базой данных.
func (e *ExplorerDB) CloseConnect() {
	err := e.connect.Close()
	if err != nil {
		log.Fatal("Ошибка при завершении соединания с БД.")
	}
}

// showTables возвращает список таблиц в БД.
func (e *ExplorerDB) showTables() (*tablesInfo, error) {
	tablesFromDB := tablesInfo{}
	rows, err := e.connect.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string

		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tablesFromDB[tableName] = nil
	}

	return &tablesFromDB, nil
}

// showColumns получает колонки и их типы для каждый таблицы БД и возвращает обновленный список таблиц.
func (e *ExplorerDB) showColumns(tablesFromDB *tablesInfo) (*tablesInfo, error) {

	for tableName := range *tablesFromDB {
		textQuery := fmt.Sprintf("DESCRIBE TABLE temp_out %s", tableName)
		rows, err := e.connect.Query(textQuery)
		if err != nil {
			return nil, err
		}

		columnsFromDB := make([]message.ColumnsType, 0)

		for rows.Next() {
			var column message.ColumnsType
			var (
				colDefaultType       sql.NullString
				colDefaultExpression sql.NullString
				colComment           sql.NullString
				colCodecExpression   sql.NullString
				colTTLExpression     sql.NullString
			)

			err = rows.Scan(
				&column.ColName,
				&column.ColType,
				&colDefaultType,
				&colDefaultExpression,
				&colComment,
				&colCodecExpression,
				&colTTLExpression)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}

			columnsFromDB = append(columnsFromDB, column)
		}

		_ = rows.Close()
		(*tablesFromDB)[tableName] = columnsFromDB

	}
	return tablesFromDB, nil
}

// LoadTables записывапет схему таблиц БД в ExplorerDB.
func (e *ExplorerDB) LoadTables() error {
	errMessage := "Не удалось получить схему базы данных. Причина: %s\n"

	tablesFromDB, err := e.showTables()
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	tablesFromDB, err = e.showColumns(tablesFromDB)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	// Запись в ExplorerDB
	e.mu.Lock()
	e.tablesFromDB = tablesFromDB
	e.mu.Unlock()
	return nil
}

// addTablesInfo добавляет схему таблицы БД в ExplorerDB.
func (e *ExplorerDB) addTablesInfo(tableName string, tableColumns []message.ColumnsType) error {
	e.mu.Lock()
	(*e.tablesFromDB)[tableName] = tableColumns
	e.mu.Unlock()
	return nil
}

// Recording создает новую таблицу если ее нет в бд или проверяет валидность полей для записи.
// Затем выполняет запись в бд.
func (e *ExplorerDB) Recording(data message.DataRecord) error {
	tableNameInterface, ok := data["tableName"]
	if !ok {
		return fmt.Errorf("Отсутствует поле tableName.\n")
	}

	tableName, ok := tableNameInterface.(string)
	if !ok {
		return fmt.Errorf("Поле tableName имеет неправильный формат.\n")
	}

	fieldsTypeInterface, ok := data["fieldsType"]
	if !ok {
		return fmt.Errorf("Отсутствует поле fieldsType.\n")
	}

	fieldsType, ok := fieldsTypeInterface.([]message.ColumnsType)
	if !ok {
		return fmt.Errorf("Поле fieldsType имеет неправильный формат.\n")
	}

	fieldsInterface, ok := data["fields"]
	if !ok {
		return fmt.Errorf("Отсутствует поле fields.\n")
	}

	fields, ok := fieldsInterface.([]message.Pair)
	if !ok {
		return fmt.Errorf("Поле fields имеет неправильный формат.\n")
	}

	// Проверка на наличие схемы таблицы.
	e.mu.RLock()
	tableInfo, ok := (*e.tablesFromDB)[tableName]
	e.mu.RUnlock()
	if ok {
		err := e.checkValid(tableInfo, fieldsType)
		if err != nil {
			return err
		}
	} else {
		err := e.createTable(tableName, fieldsType)
		if err != nil {
			return err
		}
		err = e.addTablesInfo(tableName, fieldsType)
		if err != nil {
			return err
		}
	}

	// Запись данных в БД.
	err := e.writeData(tableName, fieldsType, fields)
	if err != nil {
		return err
	}

	return nil
}

// checkValid проверяет на валидность типы колонок в существующей таблице и в новой записи.
func (e *ExplorerDB) checkValid(DBFieldsType []message.ColumnsType, recordFieldsType []message.ColumnsType) error {
	if len(recordFieldsType) != len(DBFieldsType) {
		return fmt.Errorf("Разное количество полей таблицы в БД и в новой записи.\n")
	}

	for i := 0; i < len(recordFieldsType); i++ {
		if recordFieldsType[i].ColType != DBFieldsType[i].ColType {
			return fmt.Errorf("Несоответсвие типа колонки %s: %s <> %s \n",
				recordFieldsType[i].ColName,
				recordFieldsType[i].ColType,
				DBFieldsType[i].ColType,
			)
		}
	}
	return nil
}

// createTable создает таблицу в БД если она не существует.
func (e *ExplorerDB) createTable(tableName string, fields []message.ColumnsType) error {
	var queryBuilder strings.Builder

	for _, row := range fields {
		queryBuilder.WriteString(fmt.Sprintf(" %s %s, ", row.ColName, row.ColType))
	}

	textQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) engine=Memory",
		tableName,
		strings.TrimSuffix(queryBuilder.String(), ", "))

	_, err := e.connect.Exec(textQuery)
	if err != nil {
		return err
	}

	return nil
}

// writeData записывает подготовленные данные в таблицу.
func (e *ExplorerDB) writeData(tableName string, fieldsType []message.ColumnsType, fields []message.Pair) error {
	var columnBuilder, valuesBuilder strings.Builder

	for _, row := range fieldsType {
		columnBuilder.WriteString(fmt.Sprintf(" %s, ", row.ColName))
	}

	var values []interface{}
	for _, row := range fields {
		values = append(values, row.Value)
		valuesBuilder.WriteString(" ?, ")
	}

	textQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.TrimSuffix(columnBuilder.String(), ", "),
		strings.TrimSuffix(valuesBuilder.String(), ", "))

	_, err := e.connect.Exec(textQuery, values...)

	if err != nil {
		return err
	}

	return nil
}
