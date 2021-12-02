package client

import (
	"testing"
)

var isConnectedFalse = func(m *MqttClient) bool {
	return false
}

var isConnectedTrue = func(m *MqttClient) bool {
	return true
}

var WithoutLogger = func(message string){}

var savedFuncLog = logger
var savedFuncIsConnected = isConnected

func restoreSettings() {
	logger = savedFuncLog
	isConnected = savedFuncIsConnected
}

func TestMakeMQTTClient(t *testing.T) {
	ErrMessage := "Поле %s не должно быть nil после инициализации клиента."

	m := MakeMQTTClient()

	if m.client == nil {
		t.Errorf(ErrMessage, "client")
	}

	if m.opts == nil {
		t.Errorf(ErrMessage, "opts")
	}
}

func TestSetTLSSettings(t *testing.T) {
	type testVariant struct {
		caPath string
		certPath string
		keyPath string
	}

	testVariants := []*testVariant{
		{caPath: "", certPath: "test", keyPath: "test"},
		{caPath: "test", certPath: "", keyPath: "test"},
		{caPath: "test", certPath: "test", keyPath: ""},
		{caPath: "", certPath: "", keyPath: ""},
		{caPath: "test", certPath: "test", keyPath: "test"},
	}

	for i, variant := range testVariants {
		m := MakeMQTTClient()

		err := m.SetTLSSettings(variant.caPath, variant.certPath, variant.keyPath)
		if err == nil {
			t.Errorf("Не возникает ошибка при некорректных входных данных. Номер варианта %v", i)
		}
	}
}

func TestSetBrokerUrl(t *testing.T) {
	type testVariant struct {
		inHost  string
		inPort int
		outHost string
		outScheme string
		err bool
	}

	testVariants := []*testVariant{
		{inHost: "example.com", inPort: 1883, outHost: "example.com:1883", outScheme: "tcp", err: false},
		{inHost: "example.com", inPort: 8883, outHost: "example.com:8883", outScheme: "ssl", err: false},
		{inHost: "", inPort: 0, outHost: "", outScheme: "", err: true},
	}

	for _, variant := range testVariants {
		m := MakeMQTTClient()
		err := m.SetBrokerUrl(variant.inHost, variant.inPort)
		brokersCount := len(m.opts.Servers)

		isErr := err != nil
		if isErr != variant.err {
			t.Errorf("Возникновение ошибки. Ожидание: %v, факт: %v", variant.err, isErr)
		}

		if !isErr {
			if brokersCount != 1 {
				t.Errorf("Неверное количество брокеров. Ожидание: %v, факт: %v",
					1, brokersCount)
			}

			if brokersCount > 0 {
				brokerSettings := m.opts.Servers[0]
				if brokerSettings.Scheme != variant.outScheme {
					t.Errorf("Неверное поле Scheme. Ожидание: %s, факт: %s",
						variant.outScheme, brokerSettings.Scheme)
				}

				if brokerSettings.Host != variant.outHost {
					t.Errorf("Неверное поле Host. Ожидание: %s, факт: %s",
						variant.outHost, brokerSettings.Host)
				}
			}
		}
	}
}

func TestGetCertPool(t *testing.T) {
	wrongPath := "wrongPath"
	_, err := getCertPool(wrongPath)
	if err == nil {
		t.Errorf("При некорректном пути к файлу pem не возникает ошибки.")
	}

	savedFunc := readPemFile
	defer func() {readPemFile = savedFunc}()

	readPemFile = func(pemPath string) ([]byte, error) {

		RandomPemFile := `-----BEGIN CERTIFICATE-----
MIIH/TCCBeWgAwIBAgIQaBYE3/M08XHYCnNVmcFBcjANBgkqhkiG9w0BAQsFADBy
MQswCQYDVQQGEwJVUzEOMAwGA1UECAwFVGV4YXMxEDAOBgNVBAcMB0hvdXN0b24x
ETAPBgNVBAoMCFNTTCBDb3JwMS4wLAYDVQQDDCVTU0wuY29tIEVWIFNTTCBJbnRl
cm1lZGlhdGUgQ0EgUlNBIFIzMB4XDTIwMDQwMTAwNTgzM1oXDTIxMDcxNjAwNTgz
M1owgb0xCzAJBgNVBAYTAlVTMQ4wDAYDVQQIDAVUZXhhczEQMA4GA1UEBwwHSG91
c3RvbjERMA8GA1UECgwIU1NMIENvcnAxFjAUBgNVBAUTDU5WMjAwODE2MTQyNDMx
FDASBgNVBAMMC3d3dy5zc2wuY29tMR0wGwYDVQQPDBRQcml2YXRlIE9yZ2FuaXph
dGlvbjEXMBUGCysGAQQBgjc8AgECDAZOZXZhZGExEzARBgsrBgEEAYI3PAIBAxMC
VVMwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDHheRkbb1FCc7xRKst
wK0JIGaKY8t7JbS2bQ2b6YIJDgnHuIYHqBrCUV79oelikkokRkFvcvpaKinFHDQH
UpWEI6RUERYmSCg3O8Wi42uOcV2B5ZabmXCkwdxY5Ecl51BbM8UnGdoAGbdNmiRm
SmTjcs+lhMxg4fFY6lBpiEVFiGUjGRR+61R67Lz6U4KJeLNcCm07QwFYKBmpi08g
dygSvRdUw55Jopredj+VGtjUkB4hFT4GQX/ght69Rlqz/+8u0dEQkhuUuucrqalm
SGy43HRwBfDKFwYeWM7CPMd5e/dO+t08t8PbjzVTTv5hQDCsEYIV2T7AFI9ScNxM
kh7/AgMBAAGjggNBMIIDPTAfBgNVHSMEGDAWgBS/wVqH/yj6QT39t0/kHa+gYVgp
vTB/BggrBgEFBQcBAQRzMHEwTQYIKwYBBQUHMAKGQWh0dHA6Ly93d3cuc3NsLmNv
bS9yZXBvc2l0b3J5L1NTTGNvbS1TdWJDQS1FVi1TU0wtUlNBLTQwOTYtUjMuY3J0
MCAGCCsGAQUFBzABhhRodHRwOi8vb2NzcHMuc3NsLmNvbTAfBgNVHREEGDAWggt3
d3cuc3NsLmNvbYIHc3NsLmNvbTBfBgNVHSAEWDBWMAcGBWeBDAEBMA0GCyqEaAGG
9ncCBQEBMDwGDCsGAQQBgqkwAQMBBDAsMCoGCCsGAQUFBwIBFh5odHRwczovL3d3
dy5zc2wuY29tL3JlcG9zaXRvcnkwHQYDVR0lBBYwFAYIKwYBBQUHAwIGCCsGAQUF
BwMBMEgGA1UdHwRBMD8wPaA7oDmGN2h0dHA6Ly9jcmxzLnNzbC5jb20vU1NMY29t
LVN1YkNBLUVWLVNTTC1SU0EtNDA5Ni1SMy5jcmwwHQYDVR0OBBYEFADAFUIazw5r
ZIHapnRxIUnpw+GLMA4GA1UdDwEB/wQEAwIFoDCCAX0GCisGAQQB1nkCBAIEggFt
BIIBaQFnAHcA9lyUL9F3MCIUVBgIMJRWjuNNExkzv98MLyALzE7xZOMAAAFxM0ho
bwAABAMASDBGAiEA6xeliNR8Gk/63pYdnS/vOx/CjptEMEv89WWh1/urWIECIQDy
BreHU25DzwukQaRQjwW655ZLkqCnxbxQWRiOemj9JAB1AJQgvB6O1Y1siHMfgosi
LA3R2k1ebE+UPWHbTi9YTaLCAAABcTNIaNwAAAQDAEYwRAIgGRE4wzabNRdD8kq/
vFP3tQe2hm0x5nXulowh4Ibw3lkCIFYb/3lSDplS7AcR4r+XpWtEKSTFWJmNCRbc
XJur2RGBAHUA7sCV7o1yZA+S48O5G8cSo2lqCXtLahoUOOZHssvtxfkAAAFxM0ho
8wAABAMARjBEAiB6IvboWss3R4ItVwjebl7D3yoFaX0NDh2dWhhgwCxrHwIgCfq7
ocMC5t+1ji5M5xaLmPC4I+WX3I/ARkWSyiO7IQcwDQYJKoZIhvcNAQELBQADggIB
ACeuur4QnujqmguSrHU3mhf+cJodzTQNqo4tde+PD1/eFdYAELu8xF+0At7xJiPY
i5RKwilyP56v+3iY2T9lw7S8TJ041VLhaIKp14MzSUzRyeoOAsJ7QADMClHKUDlH
UU2pNuo88Y6igovT3bsnwJNiEQNqymSSYhktw0taduoqjqXn06gsVioWTVDXysd5
qEx4t6sIgIcMm26YH1vJpCQEhKpc2y07gRkklBZRtMjThv4cXyyMX7uTcdT7AJBP
ueifCoV25JxXuo8d5139gwP1BAe7IBVPx2u7KN/UyOXdZmwMf/TmFGwDdCfsyHf/
ZsB2wLHozTYoAVmQ9FoU1JLgcVivqJ+vNlBhHXhlxMdN0j80R9Nz6EIglQjeK3O8
I/cFGm/B8+42hOlCId9ZdtndJcRJVji0wD0qwevCafA9jJlHv/jsE+I9Uz6cpCyh
sw+lrFdxUgqU58axqeK89FR+No4q0IIO+Ji1rJKr9nkSB0BqXozVnE1YB/KLvdIs
uYZJuqb2pKku+zzT6gUwHUTZvBiNOtXL4Nxwc/KT7WzOSd2wP10QI8DKg4vfiNDs
HWmB1c4Kji6gOgA5uSUzaGmq/v4VncK5Ur+n9LbfnfLc28J5ft/GotinMyDk3iar
F10YlqcOmeX1uFmKbdi/XorGlkCoMF3TDx8rmp9DBiB/
-----END CERTIFICATE-----
`
		return []byte(RandomPemFile), nil
	}

	correctPath:= "correctPath"
	result, _ := getCertPool(correctPath)
	crt := result.Subjects()

	if len(crt) != 1 {
		t.Errorf("Не сформирован crt файл")
	}
}

func TestSubscribeAll(t *testing.T) {
	logger = WithoutLogger
	isConnected = isConnectedFalse
	defer restoreSettings()

	testTopics := map[string]string{"name": "test"}
	ErrMessage := "Ожидаемое количество подписок в брокере: %v факт: %v"

	m := MakeMQTTClient()

	if len(m.topics) != 0 {
		t.Errorf(ErrMessage, 0, len(m.topics))
	}

	m.SubscribeAll(testTopics)

	if len(m.topics) != 0 {
		t.Errorf(ErrMessage, 2, len(m.topics))
	}

	isConnected = isConnectedTrue
	m.SubscribeAll(testTopics)

	if len(m.topics) != 1 {
		t.Errorf(ErrMessage, 1, len(m.topics))
	}
}

func TestUnsubscribeAll(t *testing.T) {
	logger = WithoutLogger
	isConnected = isConnectedFalse
	defer restoreSettings()

	ErrMessage := "Ожидаемое количество подписок в брокере: %v факт: %v"

	m := MakeMQTTClient()
	m.topics = append(m.topics, "test")
	m.UnsubscribeAll()

	if len(m.topics) != 1 {
		t.Errorf(ErrMessage, 1, len(m.topics))
	}

	isConnected = isConnectedTrue
	m.UnsubscribeAll()

	if len(m.topics) != 0 {
		t.Errorf(ErrMessage, 0, len(m.topics))
	}
}
