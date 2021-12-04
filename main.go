// Package main подключается к брокеру mqtt и записывает полученные сообщения в базу данных.
// Топики для получения сообщений хранятся в consul.
package main

import (
	"flag"
	"log"
	"mqtt2clickhouse/client"
	"mqtt2clickhouse/config"
	"mqtt2clickhouse/db"
	"mqtt2clickhouse/message"
)

// ReadQueue получает сообщение из очереди, преобразовывает его в подходящий формат для записи и записывает в базу.
func ReadQueue(explorer *db.ExplorerDB) {

	for {
		select {
		case msg := <-message.DataChannel:
			record, err := message.CreateRecordData(msg.Topic, msg.Value)
			if err != nil {
				log.Printf("ошибка при формировании сообщения из топика %s и тела сообщения %s",
					msg.Topic, msg.Value)
				return
			}

			err = explorer.Recording(record)
			if err != nil {
				log.Printf("ошибка при записи сообщения %v", record)
				return
			}
		case <-message.QuitChannel:
			return
		}
	}
}

func main() {
	enableTLS := flag.Bool("enableTLS", true, "Use tls connection")
	username := flag.String("username", "", "user name")
	password := flag.String("password", "", "user password")
	broker := flag.String("broker", "", "broker url")
	port := flag.Int("port", 8883, "broker port")
	consulHost := flag.String("consulHost", "", "consul url")
	DBHost := flag.String("DBHost", "", "Database url")
	flag.Parse()

	var err error

	// Подключение к брокеру MQTT
	c := client.MakeMQTTClient()
	err = c.SetBrokerUrl(*broker, *port)
	if err != nil {
		log.Fatal(err)
	}

	if *enableTLS {
		Settings, err := config.ReadSettings()
		if err != nil {
			log.Fatal(err)
		}
		err = c.SetTLSSettings(Settings.CaPath, Settings.CertPath, Settings.KeyPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	c.SignIn(*username, *password)
	c.SetHandler()
	mqttConnect := *c.Connecting()
	defer mqttConnect.Disconnect(250)

	// Подключение к Consul для получения топиков.
	kv := config.MakeKVClient()
	_, err = kv.Connect(*consulHost)
	if err != nil {
		log.Fatal(err)
	}

	// Подключение к БД
	explorer := db.ExplorerDB{}
	err = explorer.Connect(*DBHost)
	if err != nil {
		log.Fatal(err)
	}
	defer explorer.CloseConnect()

	// Загрузка схемы БД
	err = explorer.LoadTables()
	if err != nil {
		log.Fatal(err)
	}

	// читаем очередь полученных сообщений
	go ReadQueue(&explorer)

	for true {
		topicsMap, ok, err := kv.LoadTopics()
		if err != nil {
			message.QuitChannel <- 0
			log.Fatal(err)
		}

		if ok {
			c.UnsubscribeAll()
			c.SubscribeAll(topicsMap)
		}
	}
}
