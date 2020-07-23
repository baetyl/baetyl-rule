package main

import (
	"fmt"
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
	brokerConf := `
listeners:
  - address: tcp://0.0.0.0:50030
principals:
  - username: test
    password: hahaha
    permissions:
      - action: pub
        permit: ["#"]
      - action: sub
        permit: ["#"]
`
	linesConf := `
points:
  - name: baetyl-broker
    mqtt:
      address: 'tcp://127.0.0.1:50030'
      username: test
      password: hahaha
lines:
  - name: line1
    source:
      topic: broker/topic1
      qos: 1
    sink:
      topic: broker/topic2
      qos: 1
    filter:
      function: node85
`

	var brokerCfg mockBrokerConfig
	err := utils.UnmarshalYAML([]byte(brokerConf), &brokerCfg)
	assert.NoError(t, err)

	broker, err := newMockBroker(brokerCfg)
	assert.NoError(t, err)
	defer broker.close()

	mockHttp(t)
	time.Sleep(2 * time.Second)

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

	ops1 := mqtt.NewClientOptions()
	ops1.Address = "tcp://127.0.0.1:50030"
	ops1.Username = "test"
	ops1.Password = "hahaha"
	ops1.ClientID = "line1-sink"
	ops1.Subscriptions = []mqtt.Subscription{
		{
			Topic: "broker/topic2",
			QOS:   1,
		},
	}
	cli1 := newMockMqttClient(t, &ops1)
	err = cli1.start()
	assert.NoError(t, err)
	defer cli1.close()

	ops2 := mqtt.NewClientOptions()
	ops2.Address = "tcp://127.0.0.1:50030"
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
		Topic:   "broker/topic1",
		Payload: []byte(`{"name":"topic1"}`),
		QOS:     1,
	}
	err = cli2.pub(pub2)
	assert.NoError(t, err)
	msg2 := []byte(`{"hello"":"node85"}`)
	cli1.assertS2CPacket(fmt.Sprintf("<Publish ID=1 Message=<Message Topic=\"broker/topic2\" QOS=1 Retain=false Payload=%x> Dup=false>", msg2))
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
