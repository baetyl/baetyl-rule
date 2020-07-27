package extension

import (
	"bytes"
	"io/ioutil"
	gohttp "net/http"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type MqttSource struct {
	cfg    *mqtt.ClientConfig
	sink   Sink
	filter *Filter
	cli    *mqtt.Client
	log    *log.Logger
}

func NewMqttSource(cfg *mqtt.ClientConfig) (Source, error) {
	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli := mqtt.NewClient(*ops)
	source := &MqttSource{
		cfg: cfg,
		cli: cli,
		// TODO: init log
	}
	return source, nil
}

func (m *MqttSource) SendOrDrop(pkt mqtt.Packet) error {
	return m.cli.SendOrDrop(pkt)
}

func (m *MqttSource) Start(filter *Filter, sink Sink) {
	m.sink = sink
	m.filter = filter
	go m.cli.Start(m)
}

func (m *MqttSource) Close() error {
	if m.cli != nil {
		return m.cli.Close()
	}
	return nil
}

func (m *MqttSource) OnPublish(pkt *packet.Publish) error {
	var data []byte
	if m.filter != nil {
		resp, err := m.filter.Cli.PostURL(m.filter.Url, bytes.NewBuffer(pkt.Message.Payload))
		if err != nil {
			// TODO: log err or return err ?
			return err
		}
		data, err = ioutil.ReadAll(resp.Body)
		if resp.StatusCode != gohttp.StatusOK {
			// TODO: log err or return err ?
			return nil
		}
	}
	if m.cfg.Subscriptions[0].QOS == 1 && (m.sink == nil || len(data) == 0) {
		puback := packet.NewPuback()
		puback.ID = pkt.ID
		err := m.cli.SendOrDrop(puback)
		if err != nil {
			// TODO: log err or return err
			return err
		}
	}
	err := m.sink.SendOrDrop(pkt)
	if err != nil {
		// TODO: log err or return err
		return err
	}
	return nil
}

func (m *MqttSource) OnPuback(_ *packet.Puback) error {
	return nil
}

func (m *MqttSource) OnError(error error) {
	m.log.Error("error occurs", log.Error(error))
}
