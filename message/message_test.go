package message

import (
	"reflect"
	"testing"
)

func TestCheckTopic(t *testing.T) {
	type testVariant struct {
		topic  string
		result bool
		isErr  bool
	}

	testVariants := []*testVariant{
		{topic: "/balalaykajazz/plants1/out/sensors/temp_out", result: true, isErr: false},
		{topic: "balalaykajazz/plants1/out/sensors/temp_out", result: false, isErr: true},
		{topic: "/balalaykajazz/plants1/temp_out", result: false, isErr: true},
		{topic: "", result: false, isErr: true},
	}

	for i, v := range testVariants {
		ok, err := checkTopic(v.topic)
		if ok != v.result {
			t.Errorf("№%v. Топик %s должен возвращать OK с результатом %v, а вернул %v",
				i, v.topic, v.result, ok)
		}
		if (err == nil) == v.isErr {
			t.Errorf("№%v. Топик %s должен возвращать ошибку с результатом %v, а вернул %v",
				i, v.topic, v.isErr, err != nil)
		}
	}
}

func TestGetDataFromTopic(t *testing.T) {
	recordData := make(DataRecord)

	type testVariant struct {
		topic string
	}

	testVariants := []*testVariant{
		{topic: "/balalaykajazz/plants1/out/sensors/temp_out"},
		{topic: "/balalaykajazz/plants1/temp_out"},
	}

	for _, v := range testVariants {

		err := recordData.getDataFromTopic(v.topic)

		if err != nil {
			t.Errorf("Ошибка при получении полей для записи в БД из топика %s", v.topic)
		}

		tableName, _ := recordData["tableName"]
		if tableName != "temp_out" {
			t.Errorf("Поле 'tableName' должно быть заполнено 'temp_out', а заполнено %s. Топик %s",
				tableName, v.topic)
		}

		fieldsInterface, ok := recordData["fields"]
		if !ok {
			t.Errorf("Поле 'fields' должно быть заполнено.Топик %s", v.topic)
		}

		fields, ok := fieldsInterface.([]Pair)
		if !ok {
			t.Errorf("Поле 'fields' должно иметь тип %s. Топик %s", "[]Pair", v.topic)
		}

		if len(fields) != 2 {
			t.Errorf("Поле fields должно быть заполнено двумя значениями, а заполнено %v", len(fields))
		}

		if !(fields[0].Name == "client" && fields[0].Value == "balalaykajazz") {
			t.Errorf(`Поле 'fields' должно иметь поле 'client'='balalaykajazz'.
Полученные значения: %v, %v`, fields[0].Name, fields[0].Value)
		}

		if !(fields[1].Name == "device" && fields[1].Value == "plants1") {
			t.Errorf(`Поле 'fields' должно иметь поле 'device'='plants1'.
Полученные значения: %v, %v`, fields[1].Name, fields[1].Value)
		}

	}
}

func TestGetDataFromMessage(t *testing.T) {
	recordData := make(DataRecord)
	recordData["fields"] = []Pair{}
	message := `{"timestamp":"2021-11-24T20:27:23Z","value":27.8}`

	err := recordData.getDataFromMessage([]byte(message))
	if err != nil {
		t.Errorf("Ошибка при получении полей для записи в БД из сообщения %s", message)
	}

	fieldsInterface, ok := recordData["fields"]
	if !ok {
		t.Errorf("Отсутствует поле 'fields'")
	}

	fields, ok := fieldsInterface.([]Pair)
	if !ok {
		t.Errorf("Поле 'fields' должно иметь тип %s", "[]Pair")
	}

	value, ok := fields[0].Value.(float64)
	if !(fields[0].Name == "value" && value == 27.8) {
		t.Errorf("Отсутствует поле 'value' или его значение != 27.8. Name %s, value %v",
			fields[0].Name, fields[0].Value)
	}

	_, ok = recordData["fieldsType"]
	if !ok {
		t.Errorf("Отсутствует поле 'fieldsType'")
	}
}

func TestCreateColumnDesc(t *testing.T) {
	fields := []Pair{
		{Name: "client", Value: "test"},
		{Name: "device", Value: "test"},
		{Name: "value", Value: 27.8},
	}

	columns, err := createColumnDesc(fields)
	if err != nil {
		t.Errorf("Для среза %v не должно возникать ошибки", fields)
	}

	if len(columns) != 3 {
		t.Errorf("Для среза %v функция должна вернуть 3 элемента, а вернула %v", fields, len(columns))
	}

	for _, k := range columns {
		if k.ColName == "value" {
			if k.ColType != "Float64" {
				t.Errorf("Поле '%s' должно иметь тип Float64, а имеет %v", k.ColName, k.ColType)
			}
		} else {
			if k.ColType != "String" {
				t.Errorf("Поле '%s' должно иметь тип String, а имеет %v", k.ColName, k.ColType)
			}
		}
	}
}

func TestCreateRecordData(t *testing.T) {
	topic := "/balalaykajazz/plants1/out/sensors/temp_out"
	message := `{"timestamp":"2021-11-24T20:27:23Z","value":27.8}`

	recordData, err := CreateRecordData(topic, []byte(message))

	if err != nil {
		t.Errorf("Для топика %s и сообщения %s не должна возвращаться ошибка", topic, message)
	}

	var ok bool

	valueInterface, ok := recordData["tableName"]
	if !ok {
		t.Errorf("Не найдено поле 'tableName'. %v", recordData)
	} else {
		value, success := valueInterface.(string)
		valueExpected := "temp_out"
		if !success || value != valueExpected {
			t.Errorf("Поле 'tableName' не соответствует ожидаемому: %v != %v", value, valueExpected)
		}
	}

	valueInterface, ok = recordData["fields"]
	if !ok {
		t.Errorf("Не найдено поле 'fields'. %v", recordData)
	} else {
		value, success := valueInterface.([]Pair)
		valueExpected := []Pair{
			{"client", "balalaykajazz"},
			{"device", "plants1"},
			{"value", 27.8}}

		if !success || !reflect.DeepEqual(value, valueExpected) {
			t.Errorf("Поле 'fields' не соответствует ожидаемому: %v != %v", value, valueExpected)
		}
	}

	valueInterface, ok = recordData["fieldsType"]
	if !ok {
		t.Errorf("Не найдено поле 'fieldsType'. %v", recordData)
	} else {
		value, success := valueInterface.([]ColumnsType)
		valueExpected := []ColumnsType{
			{"client", "String"},
			{"device", "String"},
			{"value", "Float64"}}
		if !success || !reflect.DeepEqual(value, valueExpected) {
			t.Errorf("Поле 'fieldsType' не соответствует ожидаемому: %v != %v", value, valueExpected)
		}
	}
}

func BenchmarkCreateRecordData(b *testing.B) {
	topic := "/balalaykajazz/plants1/out/sensors/temp_out"
	message := `{"timestamp":"2021-11-24T20:27:23Z","value":27.8}`

	for i := 0; i < b.N; i++ {
		_, _ = CreateRecordData(topic, []byte(message))
	}
}
