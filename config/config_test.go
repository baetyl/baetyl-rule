package config

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	dir := t.TempDir()

	config1 := `
clients:
  - name: iotcore
    kind: mqtt
    address: 'ssl://u7isgiz.mqtt.iot.bj.baidubce.com:1884'
    username: u7isgiz/test
    ca: var/db/baetyl/cert/ca.pem
    cert: var/db/baetyl/cert/client.pem
    key: var/db/baetyl/cert/client.key
rules:
  - name: rule1
    source:
      topic: broker/topic1
      qos: 1
    target:
      client: iotcore
      topic: iotcore/topic2
      qos: 0
`

	file1 := path.Join(dir, "service1.yml")
	err := ioutil.WriteFile(file1, []byte(config1), 0644)
	assert.NoError(t, err)

	var c Config
	err = utils.LoadYAML(file1, &c)
	assert.NoError(t, err)
	assert.Equal(t, c.Clients[0].Kind, KindMqtt)
	assert.Equal(t, c.Clients[0].Name, "iotcore")
	cfg := new(mqtt.ClientConfig)
	err = c.Clients[0].Parse(cfg)
	assert.Equal(t, cfg.Address, "ssl://u7isgiz.mqtt.iot.bj.baidubce.com:1884")
	assert.Equal(t, cfg.Username, "u7isgiz/test")
	assert.Equal(t, cfg.CA, "var/db/baetyl/cert/ca.pem")
	assert.Equal(t, cfg.Cert, "var/db/baetyl/cert/client.pem")
	assert.Equal(t, cfg.Key, "var/db/baetyl/cert/client.key")
}
