package pkg

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type MqttClient struct {
	cfg     *mqtt.ClientConfig
	sibling Client
	filter  *Filter
	cli     *mqtt.Client
	log     *log.Logger
}

func NewMqttExtension(cfg *mqtt.ClientConfig) (Client, error) {
	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli := mqtt.NewClient(*ops)
	source := &MqttClient{
		cfg: cfg,
		cli: cli,
		log: log.With(log.Any("pkg", "mqtt_client"), log.Any("clientID", cfg.ClientID)),
	}
	return source, nil
}

func (m *MqttClient) SendOrDrop(pkt mqtt.Packet) error {
	return m.cli.SendOrDrop(pkt)
}

func (m *MqttClient) Start(filter *Filter, client Client) {
	m.sibling = client
	m.filter = filter
	go m.cli.Start(m)
}

func (m *MqttClient) Close() error {
	if m.cli != nil {
		return m.cli.Close()
	}
	return nil
}

func (m *MqttClient) OnPublish(pkt *packet.Publish) (err error) {
	var data []byte
	if m.filter != nil {
		data, err = m.filter.Post(pkt.Message.Payload)
		if err != nil {
			m.log.Error("error occured when invoke filter function", log.Any("url", m.filter.url), log.Error(err))
			return nil
		}
	}
	if m.cfg.Subscriptions[0].QOS == 1 && (m.sibling == nil || len(data) == 0) {
		puback := packet.NewPuback()
		puback.ID = pkt.ID
		err := m.cli.SendOrDrop(puback)
		if err != nil {
			m.log.Error("error occured when send puback", log.Error(err))
			return nil
		}
	}
	err = m.sibling.SendOrDrop(pkt)
	if err != nil {
		m.log.Error("error occured when send pkt to sink", log.Error(err))
	}
	return nil
}

func (m *MqttClient) OnPuback(pkt *packet.Puback) error {
	return m.sibling.SendOrDrop(pkt)
}

func (m *MqttClient) OnError(error error) {
	m.log.Error("error occurs", log.Error(error))
}
