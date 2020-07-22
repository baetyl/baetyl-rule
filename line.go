package main

import (
	"encoding/json"
	"fmt"
	"github.com/baetyl/baetyl-go/v2/http"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

const (
	ModulePrefix     = "baetyl-rule"
	SystemNamespace  = "baetyl-edge-system"
	BaetylBroker     = "baetyl-broker"
	BaetylFunction   = "baetyl-function"
	BaetylBrokerPort = 1883
)

type liner struct {
	cfg    Line
	source *mqtt.Client
	sink   *mqtt.Client
	filter  *http.Client
	log    *log.Logger
}

func NewLines(cfg Config) ([]*liner, error) {
	pointers := make(map[string]Point)
	for _, v := range cfg.Points {
		pointers[v.Name] = v
	}
	pointers[BaetylBroker] = getBrokerPoint()

	liners := make([]*liner, 0)
	for _, l := range cfg.Lines {
		liner, err := newLiner(l, pointers)
		if err != nil {
			return nil, err
		}
		liners = append(liners, liner)
	}

	for _, liner := range liners {
		err := liner.start()
		if err != nil {
			return nil, err
		}
	}
	return liners, nil
}

func newLiner(line Line, pointers map[string]Point) (*liner, error) {
	source, ok := pointers[line.Source.Point]
	if !ok {
		return nil, errors.Trace(errors.Errorf("point (%s) not found in rule (%s)", line.Source.Point, line.Name))
	}

	var sink Point
	if line.Sink != nil {
		sink, ok = pointers[line.Sink.Point]
		if !ok {
			return nil, errors.Trace(errors.Errorf("point (%s) not found in rule (%s)", line.Sink.Point, line.Name))
		}
	}

	subs := []mqtt.QOSTopic{
		{
			Topic: line.Source.Topic,
			QOS: line.Source.QOS,
		},
	}
	sourceCLi, err := newMqttClient(source, subs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var sinkCli *mqtt.Client
	if line.Sink != nil {
		sinkCli, err = newMqttClient(sink, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	var httpCli *http.Client
	if line.Filter != nil {
		ops := http.NewClientOptions()
		httpCli = http.NewClient(ops)
	}

	return &liner{
		cfg:    line,
		source: sourceCLi,
		sink:   sinkCli,
		filter: httpCli,
		log:    log.With(log.Any("main", "line")),
	}, nil
}

func (l *liner) start() error {
	if err := l.source.Start(l); err != nil {
		return err
	}
	return l.sink.Start(l)
}

func (l *liner) OnPublish(in *packet.Publish) (err error) {
	// filter
	var resp []byte
	if l.cfg.Filter != nil {
		// send http post
		url := fmt.Sprintf("%s.%s", BaetylFunction, SystemNamespace)
		resp, err = l.filter.PostJSON(url, in.Message.Payload)
		if err != nil {
			return
		}
	}

	// sink
	if l.sink == nil {
		return nil
	}

	pkt := packet.NewPublish()
	pkt.ID = in.ID
	pkt.Message.QOS = packet.QOS(l.cfg.Sink.QOS)
	if in.Message.QOS < pkt.Message.QOS {
		pkt.Message.QOS = in.Message.QOS
	}
	pkt.Message.Topic = l.cfg.Sink.Topic
	if err != nil {
		s := map[string]interface{}{
			"functionMessage": in,
			"errorMessage":    err.Error(),
		}
		pkt.Message.Payload, _ = json.Marshal(s)
	} else if resp != nil {
		pkt.Message.Payload = resp
		err := l.sink.Send(pkt)
		if err != nil {
			return err
		}
	}

	if in.Message.QOS == 1 && (pkt.Message.QOS == 0 || pkt.Message.Payload == nil) {
		puback := packet.NewPuback()
		puback.ID = packet.ID(in.ID)
		l.source.Send(puback)
	}
	return nil
}

func (l *liner) OnPuback(pkt *packet.Puback) error {
	return l.source.Send(pkt)
}

func (l *liner) OnError(error error) {
	l.log.Error("error occurs", log.Error(error))
}

func (l *liner) close() {
	if l.source != nil {
		l.source.Close()
	}
	if l.sink != nil {
		l.sink.Close()
	}
}

func getBrokerPoint() Point{
	// TODO: change to tls
	return Point{
		Name: BaetylBroker,
		Mqtt: &Mqtt{
			Address:        fmt.Sprintf("tcp://%s.%s:%d", BaetylBroker, SystemNamespace, BaetylBrokerPort),
			Username:       "",
			Password:       "",
		},
	}
}

func generateClientID(name string) string {
	return fmt.Sprintf("%s-%s", ModulePrefix, name)
}

func newMqttClient(point Point, subs []mqtt.QOSTopic) (*mqtt.Client, error) {
	if point.Mqtt == nil {
		return nil, errors.Trace(errors.Errorf("mqtt is not configured in point (%s)", point.Name))
	}
	cfg := mqtt.ClientConfig{
		Address:        point.Mqtt.Address,
		Username:       point.Mqtt.Username,
		Password:       point.Mqtt.Password,
		ClientID:       generateClientID(point.Name),
		DisableAutoAck: true,
		Subscriptions:  subs,
		Certificate:    point.Mqtt.Certificate,
	}
	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mqtt.NewClient(*ops), nil
}
