package rule

import (
	"errors"
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

func TestRule(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	port1, err := getFreePort()
	assert.NoError(t, err)
	port3, err := getFreePort()
	assert.NoError(t, err)
	funcSucc := "node85"
	funcErr := "python36"
	funcEmpty := "empty"

	brokerConf1 := `
listeners:
  - address: tcp://0.0.0.0:PORT1
session:
  persistence:
    store:
      source: DIR
  resendInterval: 5s
`
	brokerConf1 = strings.Replace(brokerConf1, "DIR", path.Join(dir, "test1.db"), -1)
	brokerConf1 = strings.Replace(brokerConf1, "PORT1", strconv.Itoa(port1), -1)

	rulesConf := `
clients:
  - name: mock-broker
    kind: mqtt
    address: 'tcp://127.0.0.1:PORT1'

rules:
  - name: rule1
    source:
      client: mock-broker
      topic: group1/topic1
      qos: 1
    target:
      client: mock-broker
      topic: group1/topic2
      qos: 1
    function:
      name: FUNCTIONSUCC
  - name: rule2
    source:
      client: mock-broker
      topic: group1/topic3
      qos: 1
    target:
      client: mock-broker
      topic: group1/topic4
      qos: 1
    function:
      name: FUNCTIONERR
  - name: rule3
    source:
      client: mock-broker
      topic: group1/topic5
      qos: 1
    target:
      client: mock-broker
      topic: group1/topic6
      qos: 1
    function:
      name: FUNCTIONEMPTY
  - name: rule4
    source:
      client: mock-broker
      topic: group1/topic7
      qos: 1
    target:
      client: mock-broker
      topic: group1/topic8
      qos: 1
  - name: rule5
    source:
      client: mock-broker
      topic: group1/topic9
      qos: 1
    target:
      client: mock-broker
      topic: group1/topic10
      qos: 0
  - name: rule6
    source:
      client: mock-broker
      topic: group1/topic11
      qos: 0
    target:
      client: mock-broker
      topic: group1/topic12
      qos: 1
`
	rulesConf = strings.Replace(rulesConf, "PORT1", strconv.Itoa(port1), -1)
	rulesConf = strings.Replace(rulesConf, "FUNCTIONSUCC", funcSucc, -1)
	rulesConf = strings.Replace(rulesConf, "FUNCTIONERR", funcErr, -1)
	rulesConf = strings.Replace(rulesConf, "FUNCTIONEMPTY", funcEmpty, -1)

	var brokerCfg1 mockBrokerConfig
	err = utils.UnmarshalYAML([]byte(brokerConf1), &brokerCfg1)
	assert.NoError(t, err)

	broker1, err := newMockBroker(brokerCfg1)
	assert.NoError(t, err)
	defer broker1.close()

	mockHttp(t, port3, funcSucc, funcErr, funcEmpty)

	var rulesConfig Config
	err = utils.UnmarshalYAML([]byte(rulesConf), &rulesConfig)
	assert.NoError(t, err)

	rules, err := NewRulers(rulesConfig)
	assert.NoError(t, err)
	defer func() {
		for _, rule := range rules {
			rule.Close()
		}
	}()

	rules[0].function.URL = fmt.Sprintf("http://127.0.0.1:%d/%s", port3, funcSucc)
	rules[1].function.URL = fmt.Sprintf("http://127.0.0.1:%d/%s", port3, funcErr)
	rules[2].function.URL = fmt.Sprintf("http://127.0.0.1:%d/%s", port3, funcEmpty)

	// ensure client of rule connected successfully
	time.Sleep(time.Second)

	// test rule1
	ops1 := mqtt.NewClientOptions()
	ops1.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops1.ClientID = "rule1-target"
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
	ops2.ClientID = "rule1-source"
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
	cli1.assertS2CPacketTimeout()

	pub2.ID = 101
	err = cli2.pub(pub2)
	assert.NoError(t, err)

	cli1.assertS2CPacket(fmt.Sprintf("<Publish ID=2 Message=<Message Topic=\"group1/topic2\" QOS=1 Retain=false Payload=%x> Dup=false>", msg2))
	cli1.assertS2CPacketTimeout()

	// test rule5
	ops9 := mqtt.NewClientOptions()
	ops9.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops9.ClientID = "rule5-target"
	ops9.Subscriptions = []mqtt.Subscription{
		{
			Topic: "group1/topic10",
			QOS:   1,
		},
	}
	cli9 := newMockMqttClient(t, &ops9)
	err = cli9.start()
	assert.NoError(t, err)
	defer cli9.close()

	ops10 := mqtt.NewClientOptions()
	ops10.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops10.ClientID = "rule5-source"
	cli10 := newMockMqttClient(t, &ops10)
	err = cli10.start()
	assert.NoError(t, err)
	defer cli10.close()

	pub10 := mqtt.NewPublish()
	pub10.ID = 100
	msg10 := []byte(`{"name":"topic9"}`)
	pub10.Message = packet.Message{
		Topic:   "group1/topic9",
		Payload: msg10,
		QOS:     1,
	}
	err = cli10.pub(pub10)
	assert.NoError(t, err)

	cli9.assertS2CPacket(fmt.Sprintf("<Publish ID=0 Message=<Message Topic=\"group1/topic10\" QOS=0 Retain=false Payload=%x> Dup=false>", msg10))
	cli9.assertS2CPacketTimeout()

	pub10.ID = 101
	err = cli10.pub(pub10)
	assert.NoError(t, err)

	cli9.assertS2CPacket(fmt.Sprintf("<Publish ID=0 Message=<Message Topic=\"group1/topic10\" QOS=0 Retain=false Payload=%x> Dup=false>", msg10))
	cli9.assertS2CPacketTimeout()

	// test rule6
	ops11 := mqtt.NewClientOptions()
	ops11.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops11.ClientID = "rule6-target"
	ops11.Subscriptions = []mqtt.Subscription{
		{
			Topic: "group1/topic12",
			QOS:   1,
		},
	}
	cli11 := newMockMqttClient(t, &ops11)
	err = cli11.start()
	assert.NoError(t, err)
	defer cli11.close()

	ops12 := mqtt.NewClientOptions()
	ops12.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops12.ClientID = "rule6-source"
	cli12 := newMockMqttClient(t, &ops12)
	err = cli12.start()
	assert.NoError(t, err)
	defer cli12.close()

	pub12 := mqtt.NewPublish()
	pub12.ID = 100
	msg12 := []byte(`{"name":"topic11"}`)
	pub12.Message = packet.Message{
		Topic:   "group1/topic11",
		Payload: msg12,
		QOS:     1,
	}
	err = cli12.pub(pub12)
	assert.NoError(t, err)

	cli11.assertS2CPacket(fmt.Sprintf("<Publish ID=0 Message=<Message Topic=\"group1/topic12\" QOS=0 Retain=false Payload=%x> Dup=false>", msg12))
	cli11.assertS2CPacketTimeout()

	pub12.ID = 101
	err = cli12.pub(pub12)
	assert.NoError(t, err)

	cli11.assertS2CPacket(fmt.Sprintf("<Publish ID=0 Message=<Message Topic=\"group1/topic12\" QOS=0 Retain=false Payload=%x> Dup=false>", msg12))
	cli11.assertS2CPacketTimeout()

	// test rule2
	ops3 := mqtt.NewClientOptions()
	ops3.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops3.ClientID = "rule2-target"
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
	ops4.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops4.ClientID = "rule2-source"
	cli4 := newMockMqttClient(t, &ops4)
	err = cli4.start()
	assert.NoError(t, err)
	defer cli4.close()

	pub4 := mqtt.NewPublish()
	pub4.ID = 100
	pub4.Message = packet.Message{
		Topic:   "group1/topic3",
		Payload: []byte(`{"name":"topic3"}`),
		QOS:     1,
	}
	err = cli4.pub(pub4)
	assert.NoError(t, err)
	cli3.assertS2CPacketTimeout()

	// test rule3
	ops5 := mqtt.NewClientOptions()
	ops5.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops5.ClientID = "rule3-target"
	ops5.Subscriptions = []mqtt.Subscription{
		{
			Topic: "group1/topic6",
			QOS:   1,
		},
	}
	cli5 := newMockMqttClient(t, &ops5)
	err = cli5.start()
	assert.NoError(t, err)
	defer cli5.close()

	ops6 := mqtt.NewClientOptions()
	ops6.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops6.ClientID = "rule3-source"
	cli6 := newMockMqttClient(t, &ops6)
	err = cli6.start()
	assert.NoError(t, err)
	defer cli6.close()

	pub6 := mqtt.NewPublish()
	pub6.ID = 100
	pub6.Message = packet.Message{
		Topic:   "group1/topic5",
		Payload: []byte(`{"name":"topic5"}`),
		QOS:     1,
	}
	err = cli6.pub(pub6)
	assert.NoError(t, err)
	cli5.assertS2CPacketTimeout()

	// test rule4
	ops7 := mqtt.NewClientOptions()
	ops7.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops7.ClientID = "rule4-target"
	ops7.Subscriptions = []mqtt.Subscription{
		{
			Topic: "group1/topic8",
			QOS:   1,
		},
	}
	cli7 := newMockMqttClient(t, &ops7)
	err = cli7.start()
	assert.NoError(t, err)
	defer cli7.close()

	ops8 := mqtt.NewClientOptions()
	ops8.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops8.ClientID = "rule4-source"
	cli8 := newMockMqttClient(t, &ops8)
	err = cli8.start()
	assert.NoError(t, err)
	defer cli8.close()

	pub8 := mqtt.NewPublish()
	pub8.ID = 100
	msg8 := []byte(`{"name":"topic7"}`)
	pub8.Message = packet.Message{
		Topic:   "group1/topic7",
		Payload: msg8,
		QOS:     1,
	}
	err = cli8.pub(pub8)
	assert.NoError(t, err)

	cli7.assertS2CPacket(fmt.Sprintf("<Publish ID=1 Message=<Message Topic=\"group1/topic8\" QOS=1 Retain=false Payload=%x> Dup=false>", msg8))
	cli7.assertS2CPacketTimeout()

	pub8.ID = 101
	err = cli8.pub(pub8)
	assert.NoError(t, err)

	cli7.assertS2CPacket(fmt.Sprintf("<Publish ID=2 Message=<Message Topic=\"group1/topic8\" QOS=1 Retain=false Payload=%x> Dup=false>", msg8))
	cli7.assertS2CPacketTimeout()

}

func TestSSL(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	port1, err := getFreePort()
	assert.NoError(t, err)
	port2, err := getFreePort()
	assert.NoError(t, err)

	brokerConf1 := `
listeners:
  - address: tcp://0.0.0.0:PORT1
  - address: ssl://0.0.0.0:PORT2
    ca: ../example/var/lib/baetyl/testcert/ca.crt
    key: ../example/var/lib/baetyl/testcert/server.key
    cert: ../example/var/lib/baetyl/testcert/server.crt
principals:
  - username: test
    password: hahaha
    permissions:
      - action: pub
        permit: ["#"]
      - action: sub
        permit: ["#"]
  - username: client
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
	brokerConf1 = strings.Replace(brokerConf1, "DIR", path.Join(dir, "test1.db"), -1)
	brokerConf1 = strings.Replace(brokerConf1, "PORT1", strconv.Itoa(port1), -1)
	brokerConf1 = strings.Replace(brokerConf1, "PORT2", strconv.Itoa(port2), -1)

	var brokerCfg1 mockBrokerConfig
	err = utils.UnmarshalYAML([]byte(brokerConf1), &brokerCfg1)
	assert.NoError(t, err)

	broker1, err := newMockBroker(brokerCfg1)
	assert.NoError(t, err)
	defer broker1.close()

	rulesConf := `
clients:
  - name: mock-broker
    kind: mqtt
    address: 'ssl://127.0.0.1:PORT2'
    ca: ../example/var/lib/baetyl/testcert/ca.crt
    key: ../example/var/lib/baetyl/testcert/client.key
    cert: ../example/var/lib/baetyl/testcert/client.crt
    insecureSkipVerify: true
rules:
  - name: rule1
    source:
      client: mock-broker
      topic: group1/topic1
      qos: 1
    target:
      client: mock-broker
      topic: group1/topic2
      qos: 1
`
	rulesConf = strings.Replace(rulesConf, "PORT2", strconv.Itoa(port2), -1)

	var rulesConfig Config
	err = utils.UnmarshalYAML([]byte(rulesConf), &rulesConfig)
	assert.NoError(t, err)

	rules, err := NewRulers(rulesConfig)
	assert.NoError(t, err)
	defer func() {
		for _, rule := range rules {
			rule.Close()
		}
	}()

	time.Sleep(time.Second)

	// test rule1
	ops1 := mqtt.NewClientOptions()
	ops1.Address = "tcp://127.0.0.1:" + strconv.Itoa(port1)
	ops1.ClientID = "rule1-target"
	ops1.Username = "test"
	ops1.Password = "hahaha"
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
	ops2.ClientID = "rule1-source"
	ops2.Username = "test"
	ops2.Password = "hahaha"
	cli2 := newMockMqttClient(t, &ops2)
	err = cli2.start()
	assert.NoError(t, err)
	defer cli2.close()

	pub2 := mqtt.NewPublish()
	pub2.ID = 100
	msg := []byte(`{"name":"topic1"}`)
	pub2.Message = packet.Message{
		Topic:   "group1/topic1",
		Payload: msg,
		QOS:     1,
	}
	err = cli2.pub(pub2)
	assert.NoError(t, err)
	cli1.assertS2CPacket(fmt.Sprintf("<Publish ID=1 Message=<Message Topic=\"group1/topic2\" QOS=1 Retain=false Payload=%x> Dup=false>", msg))
	cli1.assertS2CPacketTimeout()

	pub2.ID = 101
	err = cli2.pub(pub2)
	assert.NoError(t, err)

	cli1.assertS2CPacket(fmt.Sprintf("<Publish ID=2 Message=<Message Topic=\"group1/topic2\" QOS=1 Retain=false Payload=%x> Dup=false>", msg))
	cli1.assertS2CPacketTimeout()
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

func mockHttp(t *testing.T, port int, funcSucc, funcErr, funcEmpty string) {
	router := routing.New()
	router.Post("/"+funcSucc, func(c *routing.Context) error {
		c.Response.Header.Set("Content-Type", "application/json")
		c.SetBody([]byte(`{"hello"":"node85"}`))
		return nil
	})
	router.Post("/"+funcErr, func(c *routing.Context) error {
		return errors.New("func error")
	})
	router.Post("/"+funcEmpty, func(c *routing.Context) error {
		return nil
	})
	go func() {
		err := fasthttp.ListenAndServe(fmt.Sprintf(":%d", port), router.HandleRequest)
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
	case <-time.After(time.Second):
		assert.Fail(c.t, "receive common timeout")
	}
}

func (c *mockMqttClient) assertS2CPacketTimeout() {
	select {
	case pkt := <-c.s2c:
		assert.Fail(c.t, "receive unexpected packet:", pkt.String())
	case <-time.After(time.Second):
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
