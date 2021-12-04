// Package message описывает и подготавливает данные для записи в БД.
package message

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Message структура сообщения из mqtt.
type Message struct {
	Topic string
	Value []byte
}

// DataChannel канал для передачи сообщий от брокера в БД.
var DataChannel = make(chan *Message, 300)

// QuitChannel канал для остановки чтения из очереди
var QuitChannel = make(chan int)

// DataRecord описание полей для записи в БД.
type DataRecord map[string]interface{}

// ColumnsType содержит название и тип колонки в таблице БД.
type ColumnsType struct {
	ColName string
	ColType string
}

// Pair содержит название и значение данных для записи в БД.
type Pair struct {
	Name  string
	Value interface{}
}

// checkTopic проверяет валидность топика сообщения.
func checkTopic(topic string) (bool, error) {
	if !strings.HasPrefix(topic, "/") {
		return false, fmt.Errorf("Топик '%s' не содержит префикс '/'\n", topic)
	}

	if strings.Count(topic, "/") < 4 {
		return false, fmt.Errorf("Топик '%s' должен иметь формат '/client/device/../sensorName'\n", topic)
	}
	return true, nil
}

// getDataFromTopic заполняет поля для записи в БД из топика сообщения.
func (d *DataRecord) getDataFromTopic(topic string) error {
	topicFields := strings.Split(topic, "/")

	(*d)["tableName"] = topicFields[len(topicFields)-1]

	(*d)["fields"] = []Pair{
		{Name: "client", Value: topicFields[1]},
		{Name: "device", Value: topicFields[2]},
	}

	return nil
}

// getDataFromMessage заполняет поля для записи в БД из тела сообщения.
func (d *DataRecord) getDataFromMessage(message []byte) error {
	m := make(map[string]interface{})

	err := json.Unmarshal(message, &m)
	if err != nil {
		return err
	}

	valField, ok := m["value"]
	if !ok {
		return fmt.Errorf("Отсутствует поле 'value' в сообщении %s\n", message)
	}

	fieldsInterface := (*d)["fields"]
	fields, ok := fieldsInterface.([]Pair)
	if !ok {
		return fmt.Errorf("Ошибка при добавлении 'value' в структуру записи\n")
	}

	fields = append(fields, Pair{Name: "value", Value: valField})

	fieldsType, err := createColumnDesc(fields)
	if err != nil {
		return err
	}

	(*d)["fields"] = fields
	(*d)["fieldsType"] = fieldsType

	return nil
}

// createColumnDesc формирует описание таблицы для записи в бд.
func createColumnDesc(fields []Pair) ([]ColumnsType, error) {
	fieldsType := make([]ColumnsType, len(fields))
	for i := 0; i < len(fields); i++ {
		v := fields[i]

		var ok = true
		var valTypeStr string

		switch v.Value.(type) {
		case int:
			valTypeStr = fmt.Sprintf("%T", v.Value.(int))
		case float64:
			valTypeStr = fmt.Sprintf("%T", v.Value.(float64))
		case string:
			valTypeStr = fmt.Sprintf("%T", v.Value.(string))
		default:
			ok = false
		}

		if !ok {
			return nil, fmt.Errorf("Значение %s имеет некорректный формат.\n", v.Name)
		}

		ValType := fmt.Sprintf("%s", strings.Title(valTypeStr))
		fieldsType[i] = ColumnsType{ColName: v.Name, ColType: ValType}
	}

	return fieldsType, nil
}

// CreateRecordData преобразовывает данные для записи в БД.
func CreateRecordData(topic string, value []byte) (DataRecord, error) {
	var err error

	_, err = checkTopic(topic)
	if err != nil {
		return nil, err
	}

	recordData := make(DataRecord)
	err = recordData.getDataFromTopic(topic)
	if err != nil {
		return nil, err
	}
	err = recordData.getDataFromMessage(value)
	if err != nil {
		return nil, err
	}

	return recordData, nil
}
