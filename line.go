package main

import (
	"bytes"
	"io/ioutil"
	gohttp "net/http"

	"fmt"
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type liner struct {
	cfg      Line
	source   *mqtt.Client
	sink     *mqtt.Client
	filter   *http.Client
	resolver Resolver
	log      *log.Logger
}

func NewLines(cfg Config, resolver Resolver) ([]*liner, error) {
	pointers := make(map[string]Point)
	for _, v := range cfg.Points {
		pointers[v.Name] = v
	}

	liners := make([]*liner, 0)
	for _, l := range cfg.Lines {
		liner, err := newLiner(l, pointers, resolver)
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

func newLiner(line Line, pointers map[string]Point, resolver Resolver) (*liner, error) {
	source, ok := pointers[line.Source.Point]
	if !ok {
		return nil, errors.Trace(errors.Errorf("point (%s) not found in line (%s)", line.Source.Point, line.Name))
	}

	var sink Point
	if line.Sink != nil {
		sink, ok = pointers[line.Sink.Point]
		if !ok {
			return nil, errors.Trace(errors.Errorf("point (%s) not found in line (%s)", line.Sink.Point, line.Name))
		}
	}

	subs := []mqtt.QOSTopic{
		{
			Topic: line.Source.Topic,
			QOS:   uint32(line.Source.QOS),
		},
	}
	sourceCLi, err := newMqttClient(generateClientID(line.Name, "source"), source, subs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var sinkCli *mqtt.Client
	if line.Sink != nil {
		sinkCli, err = newMqttClient(generateClientID(line.Name, "sink"), sink, nil)
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
		cfg:      line,
		source:   sourceCLi,
		sink:     sinkCli,
		filter:   httpCli,
		resolver: resolver,
		log:      log.With(log.Any("main", "line"), log.Any("line", line.Name)),
	}, nil
}

func (l *liner) start() error {
	if l.sink != nil {
		if err := l.sink.Start(l); err != nil {
			return err
		}
	}
	return l.source.Start(l)
}

func (l *liner) OnPublish(in *packet.Publish) error {
	var err error
	data := in.Message.Payload
	if l.cfg.Filter != nil {
		url := l.resolver.ResolveID(l.cfg.Filter.Function)
		resp, err := l.filter.PostURL(url, bytes.NewBuffer(in.Message.Payload))
		if err != nil {
			l.log.Error("error occured when invoke filter function", log.Any("function", l.cfg.Filter.Function), log.Error(err))
			return nil
		}
		data, err = ioutil.ReadAll(resp.Body)
		if resp.StatusCode != gohttp.StatusOK {
			l.log.Error("error occured when invoke filter function", log.Any("function", l.cfg.Filter.Function), log.Any("StatusCode", resp.StatusCode))
			return nil
		}
	}

	if l.sink != nil && len(data) != 0 {
		out := mqtt.NewPublish()
		out.ID = in.ID
		out.Dup = in.Dup
		out.Message = *in.Message.Copy()
		out.Message.Payload = data
		out.Message.Topic = l.cfg.Sink.Topic
		if out.Message.QOS > mqtt.QOS(l.cfg.Sink.QOS) {
			out.Message.QOS = mqtt.QOS(l.cfg.Sink.QOS)
		}

		err = l.sink.SendOrDrop(out)
		if err != nil {
			l.log.Error("error occured when send msg to sink", log.Any("sink", l.cfg.Sink.Point), log.Error(err))
			return nil
		}

		if in.Message.QOS == 1 && l.cfg.Sink.QOS == 0 {
			if err := l.ackSource(in.ID); err != nil {
				return nil
			}
		}
	} else {
		if in.Message.QOS == 1 {
			if err := l.ackSource(in.ID); err != nil {
				return nil
			}
		}
	}
	return nil
}

func (l *liner) OnPuback(pkt *packet.Puback) error {
	return l.source.SendOrDrop(pkt)
}

func (l *liner) OnError(error error) {
	l.log.Error("error occurs", log.Error(error))
}

func (l *liner) ackSource(id mqtt.ID) error {
	puback := packet.NewPuback()
	puback.ID = id
	err := l.source.SendOrDrop(puback)
	if err != nil {
		l.log.Error("error occured when send puback to source when QOS degraded", log.Any("source", l.cfg.Source.Point), log.Error(err))
	}
	return err
}

func (l *liner) close() {
	if l.source != nil {
		l.source.Close()
	}
	if l.sink != nil {
		l.sink.Close()
	}
}

func newMqttClient(cid string, point Point, subs []mqtt.QOSTopic) (*mqtt.Client, error) {
	cfg := mqtt.ClientConfig{
		Address:              point.Mqtt.Address,
		Username:             point.Mqtt.Username,
		Password:             point.Mqtt.Password,
		ClientID:             cid,
		CleanSession:         point.Mqtt.CleanSession,
		Timeout:              point.Mqtt.Timeout,
		KeepAlive:            point.Mqtt.KeepAlive,
		MaxReconnectInterval: point.Mqtt.MaxReconnectInterval,
		MaxCacheMessages:     point.Mqtt.MaxCacheMessages,
		DisableAutoAck:       true,
		Subscriptions:        subs,
		Certificate:          point.Mqtt.Certificate,
	}

	ops, err := cfg.ToClientOptions()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mqtt.NewClient(*ops), nil
}

func generateClientID(line, name string) string {
	return fmt.Sprintf("%s-%s-%s", BaetylRule, line, name)
}
