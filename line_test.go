package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-broker/listener"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func TestLine(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	port1, err := getFreePort()
	assert.NoError(t, err)
	port2, err := getFreePort()
	assert.NoError(t, err)

	brokerConf := `
listeners:
  - address: tcp://0.0.0.0:PORT1
  - address: ssl://0.0.0.0:PORT2
    ca: example/var/lib/baetyl/testcert/ca.pem
    key: example/var/lib/baetyl/testcert/server.key
    cert: example/var/lib/baetyl/testcert/server.pem
principals:
  - username: test
    password: hahaha
    permissions:
      - action: pub
        permit: ["#"]
      - action: sub
        permit: ["#"]
  - username: client.example.org
    permissions:
      - action: pub
        permit: ["#"]
      - action: sub
        permit: ["#"]
session:
  persistence:
    store:
      source: DIR
  resendInterval: 5s
`
	brokerConf = strings.Replace(brokerConf, "DIR", path.Join(dir, "test.db"), -1)
	brokerConf = strings.Replace(brokerConf, "PORT1", strconv.Itoa(port1), -1)
	brokerConf = strings.Replace(brokerConf, "PORT2", strconv.Itoa(port2), -1)
	linesConf := `
points:
  - name: baetyl-broker
    mqtt:
      address: 'tcp://127.0.0.1:PORT1'
      username: test
      password: hahaha
  - name: iotcore
    mqtt:
      address: 'ssl://127.0.0.1:PORT2'
      username: client.example.org
      ca: example/var/lib/baetyl/testcert/ca.pem
      key: example/var/lib/baetyl/testcert/client.key
      cert: example/var/lib/baetyl/testcert/client.pem
      insecureSkipVerify: true
lines:
  - name: line1
    source:
      topic: group1/topic1
      qos: 1
    sink:
      topic: group1/topic2
      qos: 1
    filter:
      function: node85
  - name: line2
    source:
      point: iotcore
      topic: group2/topic3
      qos: 1
    sink:
      topic: group1/topic4
      qos: 1
  - name: line3
    source:
      topic: group1/topic5
      qos: 1
`

	linesConf = strings.Replace(linesConf, "PORT1", strconv.Itoa(port1), -1)
	linesConf = strings.Replace(linesConf, "PORT2", strconv.Itoa(port2), -1)

	var brokerCfg mockBrokerConfig
	err = utils.UnmarshalYAML([]byte(brokerConf), &brokerCfg)
	assert.NoError(t, err)

	broker, err := newMockBroker(brokerCfg)
	assert.NoError(t, err)
	defer broker.close()

	mockHttp(t)

	var linesConfig Config
	err = utils.UnmarshalYAML([]byte(linesConf), &linesConfig)
	assert.NoError(t, err)

	lines, err := NewLines(linesConfig, newMockResolver())
	assert.NoError(t, err)
	defer func() {
		for _, line := range lines {
			line.close()
		}
	}()

	time.Sleep(time.Second)

	// test line1
	ops1 := mqtt.NewClientOptions()
	ops1.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops1.Username = "test"
	ops1.Password = "hahaha"
	ops1.ClientID = "line1-sink"
	ops1.Subscriptions = []mqtt.Subscription{
		{
			Topic: "group1/topic2",
			QOS:   1,
		},
	}
	cli1 := newMockMqttClient(t, &ops1)
	err = cli1.start()
	assert.NoError(t, err)
	defer cli1.close()

	ops2 := mqtt.NewClientOptions()
	ops2.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops2.Username = "test"
	ops2.Password = "hahaha"
	ops2.ClientID = "line1-source"
	cli2 := newMockMqttClient(t, &ops2)
	err = cli2.start()
	assert.NoError(t, err)
	defer cli2.close()

	pub2 := mqtt.NewPublish()
	pub2.ID = 100
	pub2.Message = packet.Message{
		Topic:   "group1/topic1",
		Payload: []byte(`{"name":"topic1"}`),
		QOS:     1,
	}
	err = cli2.pub(pub2)
	assert.NoError(t, err)
	msg2 := []byte(`{"hello"":"node85"}`)
	cli1.assertS2CPacket(fmt.Sprintf("<Publish ID=1 Message=<Message Topic=\"group1/topic2\" QOS=1 Retain=false Payload=%x> Dup=false>", msg2))

	// test line2
	ops3 := mqtt.NewClientOptions()
	ops3.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops3.Username = "test"
	ops3.Password = "hahaha"
	ops3.ClientID = "line2-sink"
	ops3.Subscriptions = []mqtt.Subscription{
		{
			Topic: "group1/topic4",
			QOS:   1,
		},
	}
	cli3 := newMockMqttClient(t, &ops3)
	err = cli3.start()
	assert.NoError(t, err)
	defer cli3.close()

	ops4 := mqtt.NewClientOptions()
	ops4.Address = "ssl://127.0.0.1:" + strconv.Itoa(port2)
	ops4.Username = "client.example.org"
	ops4.ClientID = "line2-source"
	certificate := utils.Certificate{
		CA:                 "example/var/lib/baetyl/testcert/ca.pem",
		Key:                "example/var/lib/baetyl/testcert/client.key",
		Cert:               "example/var/lib/baetyl/testcert/client.pem",
		InsecureSkipVerify: true,
	}
	tlsconfig, err := utils.NewTLSConfigClient(certificate)
	assert.NoError(t, err)
	ops4.TLSConfig = tlsconfig
	cli4 := newMockMqttClient(t, &ops4)
	err = cli4.start()
	assert.NoError(t, err)
	defer cli4.close()

	pub4 := mqtt.NewPublish()
	pub4.ID = 100
	msg4 := `{"name":"topic3"}`
	pub4.Message = packet.Message{
		Topic:   "group2/topic3",
		Payload: []byte(msg4),
		QOS:     1,
	}
	err = cli4.pub(pub4)
	assert.NoError(t, err)
	cli3.assertS2CPacket(fmt.Sprintf("<Publish ID=1 Message=<Message Topic=\"group1/topic4\" QOS=1 Retain=false Payload=%x> Dup=false>", msg4))

	// test line3
	ops5 := mqtt.NewClientOptions()
	ops5.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops5.Username = "test"
	ops5.Password = "hahaha"
	ops5.ClientID = "line3-source"
	cli5 := newMockMqttClient(t, &ops5)
	err = cli5.start()
	assert.NoError(t, err)
	defer cli5.close()

	pub5 := mqtt.NewPublish()
	pub5.ID = 100
	msg5 := `{"name":"topic5"}`
	pub4.Message = packet.Message{
		Topic:   "group1/topic5",
		Payload: []byte(msg5),
		QOS:     1,
	}
	err = cli5.pub(pub4)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
}

type mockBrokerConfig struct {
	Listeners []listener.Listener `yaml:"listeners" json:"listeners"`
	Session   session.Config      `yaml:",inline" json:",inline"`
}

type mockBroker struct {
	ses *session.Manager
	lis *listener.Manager
}

func newMockBroker(cfg mockBrokerConfig) (*mockBroker, error) {
	var err error
	b := &mockBroker{}

	b.ses, err = session.NewManager(cfg.Session)
	if err != nil {
		return nil, err
	}

	b.lis, err = listener.NewManager(cfg.Listeners, b.ses)
	if err != nil {
		b.close()
		return nil, err
	}
	return b, nil
}

func (b *mockBroker) close() {
	if b.lis != nil {
		b.lis.Close()
	}
	if b.ses != nil {
		b.ses.Close()
	}
}

type mockResolver struct{}

func newMockResolver() Resolver {
	return new(mockResolver)
}

func (r *mockResolver) ResolveID(name string) string {
	return fmt.Sprintf("http://127.0.0.1:50040/%s", name)
}

func mockHttp(t *testing.T) {
	router := routing.New()
	router.Post("/node85", func(c *routing.Context) error {
		c.Response.Header.Set("Content-Type", "application/json")
		c.SetBody([]byte(`{"hello"":"node85"}`))
		return nil
	})
	go func() {
		err := fasthttp.ListenAndServe(":50040", router.HandleRequest)
		assert.NoError(t, err)
	}()
}

type mockMqttClient struct {
	t   *testing.T
	cli *mqtt.Client
	s2c chan mqtt.Packet
}

func newMockMqttClient(t *testing.T, ops *mqtt.ClientOptions) *mockMqttClient {
	cli := mqtt.NewClient(*ops)
	return &mockMqttClient{
		t:   t,
		cli: cli,
		s2c: make(chan mqtt.Packet, 20),
	}
}

func (c *mockMqttClient) start() error {
	return c.cli.Start(c)
}

func (c *mockMqttClient) pub(pkt mqtt.Packet) error {
	return c.cli.Send(pkt)
}

func (c *mockMqttClient) close() error {
	return c.cli.Close()
}

func (c *mockMqttClient) OnPublish(in *packet.Publish) error {
	c.s2c <- in
	return nil
}

func (c *mockMqttClient) assertS2CPacket(expect string) {
	select {
	case pkt := <-c.s2c:
		assert.NotNil(c.t, pkt)
		assert.Equal(c.t, expect, pkt.String())
	case <-time.After(10 * time.Minute):
		assert.Fail(c.t, "receive common timeout")
	}
}

func (c *mockMqttClient) OnPuback(pkt *packet.Puback) error {
	return nil
}

func (c *mockMqttClient) OnError(error error) {
	return
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
