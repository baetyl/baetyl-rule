package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	config1 := `
points:
  - name: iotcore
    mqtt:
      address: 'ssl://u7isgiz.mqtt.iot.bj.baidubce.com:1884'
      clientid: 11dd7422353c46fc8851ef8fb7114534
      username: u7isgiz/test
      ca: var/db/baetyl/cert/ca.pem
      cert: var/db/baetyl/cert/client.pem
      key: var/db/baetyl/cert/client.key
  - name: baetyl-broker
    mqtt:
      address: 'baetyl-broker:1883'
      username: test
      password: hahaha
lines:
  - name: line1
    source:
      topic: broker/topic1
      qos: 1
    sink:
      point: iotcore
      topic: iotcore/topic2
      qos: 0
  - name: line2
    source:
      point: iotcore
      topic: iotcore/topic3
      qos: 0
    sink:
      topic: broker/topic4
      qos: 1
  - name: line3
    source:
      topic: broker/topic5
      qos: 0
    sink:
      topic: broker/topic6
      qos: 1
    filter:
      function: node85
`

	file1 := path.Join(dir, "service1.yml")
	err = ioutil.WriteFile(file1, []byte(config1), 0644)
	assert.NoError(t, err)

	var c Config
	err = utils.LoadYAML(file1, &c)
	assert.NoError(t, err)

	assert.Len(t, c.Points, 2)
	assert.Equal(t, "iotcore", c.Points[0].Name)
	assert.Equal(t, "baetyl-broker", c.Points[1].Name)
	assert.NotNil(t, c.Points[0].Mqtt)
	assert.NotNil(t, c.Points[1].Mqtt)
	assert.Equal(t, c.Lines[0].Name, "line1")
	assert.Equal(t, c.Lines[0].Source.Point, "baetyl-broker")
	assert.Equal(t, c.Lines[0].Source.Topic, "broker/topic1")
	assert.Equal(t, c.Lines[0].Source.QOS, 1)
	assert.Equal(t, c.Lines[0].Sink.Point, "iotcore")
	assert.Equal(t, c.Lines[0].Sink.Topic, "iotcore/topic2")
	assert.Equal(t, c.Lines[0].Sink.QOS, 0)
	assert.Equal(t, c.Lines[1].Name, "line2")
	assert.Equal(t, c.Lines[1].Source.Point, "iotcore")
	assert.Equal(t, c.Lines[1].Source.Topic, "iotcore/topic3")
	assert.Equal(t, c.Lines[1].Source.QOS, 0)
	assert.Equal(t, c.Lines[1].Sink.Point, "baetyl-broker")
	assert.Equal(t, c.Lines[1].Sink.Topic, "broker/topic4")
	assert.Equal(t, c.Lines[1].Sink.QOS, 1)
	assert.Equal(t, c.Lines[2].Name, "line3")
	assert.Equal(t, c.Lines[2].Source.Point, "baetyl-broker")
	assert.Equal(t, c.Lines[2].Source.Topic, "broker/topic5")
	assert.Equal(t, c.Lines[2].Source.QOS, 0)
	assert.Equal(t, c.Lines[2].Sink.Point, "baetyl-broker")
	assert.Equal(t, c.Lines[2].Sink.Topic, "broker/topic6")
	assert.Equal(t, c.Lines[2].Sink.QOS, 1)

	config2 := `
points:
  - name: iotcore
  - name: baetyl-broker
    mqtt:
      address: 'baetyl-broker:1883'
      username: test
      password: hahaha
`

	file2 := path.Join(dir, "service2.yml")
	err = ioutil.WriteFile(file2, []byte(config2), 0644)
	assert.NoError(t, err)

	var c2 Config
	err = utils.LoadYAML(file2, &c2)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Points[0].Mqtt: zero value")

	config3 := `
points:
 - name: baetyl-broker
   mqtt:
     username: test
     password: hahaha
`

	file3 := path.Join(dir, "service3.yml")
	err = ioutil.WriteFile(file3, []byte(config3), 0644)
	assert.NoError(t, err)

	var c3 Config
	err = utils.LoadYAML(file3, &c3)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Points[0].Mqtt.Address: zero value")

	config4 := `
lines:
  - source:
      topic: broker/topic1
      qos: 1
    sink:
      point: iotcore
      topic: iotcore/topic2
      qos: 0
`

	file4 := path.Join(dir, "service4.yml")
	err = ioutil.WriteFile(file4, []byte(config4), 0644)
	assert.NoError(t, err)

	var c4 Config
	err = utils.LoadYAML(file4, &c4)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Lines[0].Name: zero value")

	config5 := `
lines:
  - name: line1
    source:
      topic: broker/topic1
      qos: 1
    sink:
      point: iotcore
      topic: iotcore/topic2
      qos: 0
`

	file5 := path.Join(dir, "service5.yml")
	err = ioutil.WriteFile(file5, []byte(config5), 0644)
	assert.NoError(t, err)

	var c5 Config
	err = utils.LoadYAML(file5, &c5)
	assert.NoError(t, err)
	assert.Equal(t, c5.Lines[0].Source.Point, "baetyl-broker")
	assert.Equal(t, c5.Lines[0].Sink.Point, "iotcore")

	config6 := `
lines:
  - name: line1
    source:
      qos: 1
    sink:
      point: iotcore
      topic: iotcore/topic2
      qos: 0
`

	file6 := path.Join(dir, "service6.yml")
	err = ioutil.WriteFile(file6, []byte(config6), 0644)
	assert.NoError(t, err)

	var c6 Config
	err = utils.LoadYAML(file6, &c6)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Lines[0].Source.Topic: zero value")

	config7 := `
lines:
  - name: line1
    source:
      topic: broker/topic1
      qos: 3
    sink:
      point: iotcore
      topic: iotcore/topic2
      qos: 0
`

	file7 := path.Join(dir, "service7.yml")
	err = ioutil.WriteFile(file7, []byte(config7), 0644)
	assert.NoError(t, err)

	var c7 Config
	err = utils.LoadYAML(file7, &c7)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Lines[0].Source.QOS: greater than max")
}
