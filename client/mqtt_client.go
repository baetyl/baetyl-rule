package client

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-rule/v2/config"
)

type MqttClient struct {
	cli    *mqtt.Client
	logger *log.Logger
}

func NewMqttClient(cfg *mqtt.ClientConfig) (Client, error) {
	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli := mqtt.NewClient(ops)
	source := &MqttClient{
		cli:    cli,
		logger: log.With(log.Any("client", "mqtt")),
	}
	return source, nil
}

func (m *MqttClient) SendOrDrop(pkt *config.TargetMsg) error {
	out := mqtt.NewPublish()
	out.Message = packet.Message{
		Topic:   pkt.Topic,
		Payload: pkt.Data,
		QOS:     mqtt.QOS(0),
	}
	if _, ok := pkt.Meta["ID"]; ok {
		out.ID = pkt.Meta["ID"].(packet.ID)
		out.Dup = pkt.Meta["Dup"].(bool)
		out.Message.Retain = pkt.Meta["Retain"].(bool)
		out.Message.QOS = pkt.Meta["QoS"].(mqtt.QOS)
	}
	return m.cli.SendOrDrop(out)
}

func (m *MqttClient) SendPubAck(pkt mqtt.Packet) error {
	return m.cli.SendOrDrop(pkt)
}

func (m *MqttClient) Start(obs mqtt.Observer) error {
	return m.cli.Start(obs)
}

func (m *MqttClient) ResetClient(cfg *mqtt.ClientConfig) {
	ops := &mqtt.ClientOptions{
		ClientID: cfg.ClientID,
		Username: cfg.Username,
		Password: cfg.Password,
	}
	m.cli.ResetClient(ops)
}

func (m *MqttClient) SetReconnectCallback(callback mqtt.ReconnectCallback) {
	m.cli.SetReconnectCallback(callback)
}

func (m *MqttClient) Close() error {
	if m.cli != nil {
		return m.cli.Close()
	}
	return nil
}
