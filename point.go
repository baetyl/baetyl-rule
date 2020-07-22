package main

import (
	"fmt"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

const (
	ModulePrefix     = "baetyl-rule"
	SystemNamespace  = "baetyl-edge-system"
	BaetylBroker     = "baetyl-broker"
	BaetylBrokerPort = 1883
)

type pointer struct {
	cfg  Point
	mqtt *mqtt.Client
	log  *log.Logger
}

func NewPoints(cfg Config) (map[string]*pointer, error) {
	pointers := make(map[string]*pointer)
	cfg.Points = append(cfg.Points, getBrokerPoint())
	for _, l := range cfg.Points {
		pointer, err := newPointer(l, cfg.Lines)
		if err != nil {
			return nil, err
		}
		pointers[l.Name] = pointer
	}
	return pointers, nil
}

func newPointer(point Point, lines []Line) (*pointer, error) {
	if point.Mqtt == nil {
		return nil, errors.Trace(errors.Errorf("mqtt is not configured in point (%s)", point.Name))
	}
	var subs []mqtt.QOSTopic
	for _, line := range lines {
		if point.Name == line.Source.Point {
			sub := mqtt.QOSTopic{
				Topic: line.Source.Topic,
				QOS:   line.Source.QOS,
			}
			subs = append(subs, sub)
		}
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
	mqtt := mqtt.NewClient(*ops)
	if mqtt != nil {
		return nil, errors.Trace(err)
	}
	return &pointer{
		cfg:  point,
		mqtt: mqtt,
		log:  log.With(log.Any("main", "point")),
	}, nil
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
