package extension

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type MqttSink struct {
	cfg    *mqtt.ClientConfig
	source Source
	cli    *mqtt.Client
	log    *log.Logger
}

func NewMqttSink(cfg *mqtt.ClientConfig) (Sink, error) {
	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli := mqtt.NewClient(*ops)
	source := &MqttSink{
		cli: cli,
		// TODO: init log
	}
	return source, nil
}

func (m *MqttSink) SendOrDrop(pkt mqtt.Packet) error {
	var in *mqtt.Publish
	var ok bool
	if in, ok = pkt.(*mqtt.Publish); !ok {
		return errors.Trace(errors.Errorf("can't convert pkt to *mqtt.Publish"))
	}

	out := mqtt.NewPublish()
	out.ID = in.ID
	out.Dup = in.Dup
	out.Message = *in.Message.Copy()
	return m.cli.SendOrDrop(out)
}

func (m *MqttSink) Start(source Source) {
	m.source = source
	go m.cli.Start(m)
}

func (m *MqttSink) Close() error {
	if m.cli != nil {
		return m.cli.Close()
	}
	return nil
}

func (m *MqttSink) OnPublish(_ *packet.Publish) error {
	return nil
}

func (m *MqttSink) OnPuback(pkt *packet.Puback) error {
	return m.source.SendOrDrop(pkt)
}

func (m *MqttSink) OnError(error error) {
	m.log.Error("error occurs", log.Error(error))
}
