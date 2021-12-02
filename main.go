package main

import (
	"flag"
	"log"
	"mqtt2clickhouse/client"
	"mqtt2clickhouse/config"
)

func main() {
	enableTLS := flag.Bool("enableTLS", true, "Use tls connection")
	username := flag.String("username", "", "user name")
	password := flag.String("password", "", "user password")
	broker := flag.String("broker", "", "broker url")
	port := flag.Int("port", 8883, "broker port")
	consulHost := flag.String("consulHost", "", "consul url")
	//DBHost := flag.String("DBHost", "", "Database url")
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

	for true {
		topicsMap, err := kv.LoadTopics()
		if err != nil {
			log.Fatal(err)
		}

		c.UnsubscribeAll()
		c.SubscribeAll(topicsMap)
	}
}
