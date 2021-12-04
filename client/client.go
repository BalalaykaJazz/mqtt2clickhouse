// Package client управляет подключением к брокеру mqtt и получением данных.
package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"io/ioutil"
	"log"
	"mqtt2clickhouse/message"
)

// MqttClient структура для подключения к брокеру mqtt.
type MqttClient struct {
	opts   *mqtt.ClientOptions
	client mqtt.Client
	topics []string
}

// messagePubHandler обработчик событий при получении сообщений из mqtt.
var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	message.DataChannel <- &message.Message{Topic: msg.Topic(), Value: msg.Payload()}
}

// connectHandler обработчик событий при подключении к mqtt.
var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	logger("Соединение с mqtt установлено")
}

// connectLostHandler обработчик событий при потере соединения с mqtt.
var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	logger(fmt.Sprintf("Соединение с mqtt потеряно: %v", err))
}

// readPemFile читает данные pem сертификата из файла
var readPemFile = func(pemPath string) ([]byte, error) {
	pemData, err := ioutil.ReadFile(pemPath)
	return pemData, err
}

// getCertPool преобразовывает сертификат из pem в crt.
func getCertPool(pemPath string) (*x509.CertPool, error) {
	certs := x509.NewCertPool()

	pemData, err := readPemFile(pemPath)
	if err != nil {
		return nil, err
	}
	certs.AppendCertsFromPEM(pemData)
	return certs, nil
}

// SetTLSSettings добавляет сертификаты TLS в настройки подключения к mqtt.
// Если нужных сертификатов нет, то работа программы завершается.
func (m *MqttClient) SetTLSSettings(caPath, certPath, keyPath string) error {
	if caPath == "" {
		return fmt.Errorf("Не указан CA cert\n")
	} else if certPath == "" {
		return fmt.Errorf("Не указан client certificate\n")
	} else if keyPath == "" {
		return fmt.Errorf("Не указан client key\n")
	}

	TLSConfig := &tls.Config{InsecureSkipVerify: true}

	// CA certificate
	certPool, err := getCertPool(certPath)
	if err != nil {
		return err
	}
	TLSConfig.RootCAs = certPool

	// Client certificate/key pair
	certPair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return err
	}
	TLSConfig.Certificates = []tls.Certificate{certPair}

	m.opts.SetTLSConfig(TLSConfig)
	return nil
}

// SetBrokerUrl добавляет url брокера в настройки подключения к mqtt.
func (m *MqttClient) SetBrokerUrl(host string, port int) error {
	if host == "" || port <= 0 {
		return fmt.Errorf("Указаны некорректные настройки брокера для подключения: %s %v\n",
			host, port)
	}

	scheme := "tcp"
	if port == 8883 {
		scheme = "ssl"
	}
	m.opts.AddBroker(fmt.Sprintf("%s://%s:%d", scheme, host, port))

	return nil
}

// SignIn добавляет учетную информацию пользователя в настройки подключения к mqtt.
func (m *MqttClient) SignIn(username, password string) {
	m.opts.SetUsername(username)
	m.opts.SetPassword(password)
}

// SetHandler добавляет обработчки событий в настройки подключения к mqtt.
func (m *MqttClient) SetHandler() {
	m.opts.SetDefaultPublishHandler(messagePubHandler)
	m.opts.OnConnect = connectHandler
	m.opts.OnConnectionLost = connectLostHandler
}

// Connecting подключается к mqtt используя полученные ранее настройки.
func (m *MqttClient) Connecting() *mqtt.Client {
	m.client = mqtt.NewClient(m.opts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	logger("Подключение к клиенту успешно завершено")
	return &m.client
}

// SubscribeAll подписывается на указанные топики.
func (m *MqttClient) SubscribeAll(topics map[string]string) {
	if len(topics) == 0 || !isConnected(m) {
		return
	}

	for _, topic := range topics {
		token := m.client.Subscribe(topic, 1, nil)
		token.Wait()
		m.topics = append(m.topics, topic)
		logger(fmt.Sprintf("Подписка на топик %s", topic))
	}
}

// UnsubscribeAll отписывается от всех топиков.
func (m *MqttClient) UnsubscribeAll() {
	if len(m.topics) == 0 || !isConnected(m) {
		return
	}

	logger("Отписка от всех топиков")
	m.client.Unsubscribe(m.topics...)
	m.topics = nil
}

// Publish отправляет сообщение в указанный топик.
func (m *MqttClient) Publish(topic string, message string) {
	if !isConnected(m) {
		return
	}

	token := m.client.Publish(topic, 0, false, message)
	token.Wait()
}

// isConnected проверка подключения к брокеру.
var isConnected = func(m *MqttClient) bool {
	return m.client.IsConnected()
}

// logger ведет лог событий в ходе работы программы
var logger = func(message string) {
	log.Println(message)
}

// MakeMQTTClient функция возвращает объект для подключения к mqtt.
func MakeMQTTClient() MqttClient {
	opts := mqtt.NewClientOptions()
	return MqttClient{opts: opts, client: mqtt.NewClient(opts), topics: nil}
}
