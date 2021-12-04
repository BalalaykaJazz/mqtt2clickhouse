// Package config обрабатывает настройки для подключения к внешним источникам.
package config

import (
	"encoding/json"
	"fmt"
	consulApi "github.com/hashicorp/consul/api"
	"io/ioutil"
)

const (
	defaultTLSConfigFile = "config/configTLS.json"
	topicsPathInKV       = "mqttClient/topics"
)

// configTLS содержит пути к сертификатам для TLS.
type configTLS struct {
	CaPath   string `json:"caPath"`
	CertPath string `json:"certPath"`
	KeyPath  string `json:"keyPath"`
}

// StoreKV содержит клиент подключения к consul и последний полученный индекс.
type StoreKV struct {
	client    *consulApi.Client
	LastIndex uint64
}

// readSettingsFile возвращает прочитанный файл настроек.
var readSettingsFile = func(filePath string) ([]byte, error) {
	data, err := ioutil.ReadFile(filePath)
	return data, err
}

// ReadSettings читает конфигурационный файл в структуру и возвращает ее.
func ReadSettings() (*configTLS, error) {
	data, err := readSettingsFile(defaultTLSConfigFile)
	if err != nil {
		return nil, fmt.Errorf("Ошибка при чтении файла: %s\n", err)
	}

	var cfg = &configTLS{}
	err = json.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("Ошибка при чтении настроек из файла %s. %s\n",
			defaultTLSConfigFile, err)
	}

	return cfg, nil
}

// MakeKVClient возвращает объект для подключения к consul.
func MakeKVClient() StoreKV {
	return StoreKV{LastIndex: 0}
}

// Connect подключается к consul и возвращает клиент.
func (s *StoreKV) Connect(address string) (*consulApi.Client, error) {
	var err error
	consulCfg := consulApi.DefaultConfig()
	consulCfg.Address = address
	s.client, err = consulApi.NewClient(consulCfg)

	if err != nil {
		return nil, fmt.Errorf("Ошибка при подключении к consul %s\n", err)
	}

	return s.client, nil
}

// LoadConfig получает данные из consul и возвращает их в виде map.
func (s *StoreKV) LoadConfig(fieldName string) (map[string]string, bool, error) {
	QueryOpt := &consulApi.QueryOptions{WaitIndex: s.LastIndex}

	KVPair, _, err := s.client.KV().Get(fieldName, QueryOpt)
	if err != nil {
		return nil, false, fmt.Errorf("Ошибка при получении данных из consul %s\n", err)
	} else if KVPair == nil {
		return nil, false, fmt.Errorf("Ошибка при получении данных из consul: данные не найдены.\n")
	}

	var kv map[string]string
	err = json.Unmarshal(KVPair.Value, &kv)
	if err != nil {
		return nil, false, fmt.Errorf("Ошибка при чтении настроек из consul %s \n", err)
	}

	var ok bool

	if KVPair.ModifyIndex != s.LastIndex {
		s.LastIndex = KVPair.ModifyIndex
		ok = true
	}

	return kv, ok, nil
}

// LoadTopics получает список топиков из consul.
func (s *StoreKV) LoadTopics() (map[string]string, bool, error) {
	return s.LoadConfig(topicsPathInKV)
}
