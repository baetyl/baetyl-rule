package main

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type liner struct {
	cfg    Line
	source *mqtt.Client
	sink   *mqtt.Client
	log    *log.Logger
}

func NewLines(cfg Config, pointers map[string]*pointer) ([]*liner, error) {
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

func newLiner(line Line, pointers map[string]*pointer) (*liner, error) {
	source, ok := pointers[line.Source.Point]
	if !ok {
		return nil, errors.Trace(errors.Errorf("point (%s) not found in rule (%s)", line.Source.Point, line.Name))
	}

	var sink *pointer
	if line.Sink != nil {
		sink, ok = pointers[line.Sink.Point]
		if !ok {
			return nil, errors.Trace(errors.Errorf("point (%s) not found in rule (%s)", line.Sink.Point, line.Name))
		}
	}

	return &liner{
		cfg:    line,
		source: source.mqtt,
		sink:   sink.mqtt,
		log:    log.With(log.Any("main", "line")),
	}, nil
}

func (l *liner) start() error {
	hubHandler := mqtt.NewHandlerWrapper(
		func(p *packet.Publish) error {
			return rr.remote.Send(p)
		},
		func(p *packet.Puback) error {
			return rr.remote.Send(p)
		},
		func(e error) {
			rr.log.Errorln("hub error:", e.Error())
		},
	)
	if err := rr.hub.Start(hubHandler); err != nil {
		return err
	}
	remoteHandler := mqtt.NewHandlerWrapper(
		func(p *packet.Publish) error {
			return rr.hub.Send(p)
		},
		func(p *packet.Puback) error {
			return rr.hub.Send(p)
		},
		func(e error) {
			rr.log.Errorln("remote error:", e.Error())
		},
	)
	if err := rr.remote.Start(remoteHandler); err != nil {
		return err
	}
	return nil
}

func (l *liner) close() {
	if l.source != nil {
		l.source.Close()
	}
	if l.sink != nil {
		l.sink.Close()
	}
}
