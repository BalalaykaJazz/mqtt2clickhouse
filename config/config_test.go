package config

import (
	"testing"
)

func TestMakeKVClient(t *testing.T) {
	c := MakeKVClient()

	if c.LastIndex != 0 {
		t.Errorf("LastIndex при создании клиента должен быть равен 0, а равен %v", c.LastIndex )
	}
}

func TestReadSettings(t *testing.T) {

	savedFunc := readSettingsFile
	defer func() {readSettingsFile = savedFunc}()

	readSettingsFile = func(filePath string) ([]byte, error) {
		configFile := `{
  "caPath": "config/test_ca.pem",
  "certPath": "config/test_client.pem",
  "keyPath": "config/test_client.key"}`

		return []byte(configFile), nil
	}

	cfg, err := ReadSettings()
	if err != nil {
		t.Errorf("Не удалось прочитать файл с настройками")
	}

	expectedCfg := configTLS{
		"config/test_ca.pem",
		"config/test_client.pem",
		"config/test_client.key"}

	if *cfg != expectedCfg {
		t.Errorf("Получена структура настроек %s вместо ожидаемой %s", cfg, expectedCfg)
	}

}

func TestConnect(t *testing.T) {
	s := StoreKV{LastIndex: 0}
	c, err := s.Connect("testAddress")

	errCreateClient := "Ошибка при создании клиента для StoreKV"

	if err != nil {
		t.Errorf(errCreateClient)
	}

	if c == nil {
		t.Errorf(errCreateClient)
	}

	if s.client == nil {
		t.Errorf("Созданный клиент не добавлен в структуру StoreKV")
	} else if s.client != c {
		t.Errorf("Созданный клиент не соответствует клиенту в структуре StoreKV")
	}
}