package line

import (
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/baetyl/baetyl-rule/extension"
)

func GetSource(l Line, point Point) (s extension.Source, err error) {
	switch point.Kind {
	case KindMqtt:
		cfg := new(mqtt.ClientConfig)
		err = utils.SetDefaults(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = point.Parse(cfg)
		cfg.ClientID = generateClientID(l.Name, l.Source.Point)
		cfg.DisableAutoAck = true
		qos := l.Source.QOS
		if l.Sink.QOS < qos {
			qos = l.Sink.QOS
		}
		cfg.Subscriptions = []mqtt.QOSTopic{
			{
				QOS:   uint32(l.Source.QOS),
				Topic: l.Source.Topic,
			},
		}
		s, err = extension.NewMqttSource(cfg)
	default:
		err = errors.Trace(errors.Errorf("point kind (%s) is not supported"))
	}
	return s, err
}

func GetSink(l Line, point Point) (s extension.Sink, err error) {
	switch point.Kind {
	case KindMqtt:
		cfg := new(mqtt.ClientConfig)
		err = utils.SetDefaults(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = point.Parse(cfg)
		cfg.ClientID = generateClientID(l.Name, l.Sink.Point)
		cfg.DisableAutoAck = true
		s, err = extension.NewMqttSink(cfg)
	default:
		err = errors.Trace(errors.Errorf("point kind (%s) is not supported"))
	}
	return s, err
}
