package client

import (
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type MqttClient struct {
	cli *mqtt.Client
}

func NewMqttClient(cfg *mqtt.ClientConfig) (Client, error) {
	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli := mqtt.NewClient(ops)
	source := &MqttClient{
		cli: cli,
	}
	return source, nil
}

func (m *MqttClient) SendOrDrop(method string, pkt *mqtt.Publish) error {
	return m.cli.SendOrDrop(pkt)
}

func (m *MqttClient) SendPubAck(pkt mqtt.Packet) error {
	return m.cli.SendOrDrop(pkt)
}

func (m *MqttClient) Start(obs mqtt.Observer) {
	m.cli.Start(obs)
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
