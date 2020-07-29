package rule

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-rule/client"
)

type Ruler struct {
	cfg      RuleInfo
	source   client.Client
	target   client.Client
	function *client.Function
	log      *log.Logger
}

func NewRulers(cfg Config) ([]*Ruler, error) {
	clients := make(map[string]ClientInfo)
	for _, v := range cfg.Clients {
		clients[v.Name] = v
	}

	// baetyl-broker client is the mqtt broker in edge
	clients[BaetylBroker] = getBrokerClient()

	for _, rule := range cfg.Rules {
		_, ok := clients[rule.Source.Client]
		if !ok {
			return nil, errors.Trace(errors.Errorf("client (%s) not found in rule (%s)", rule.Source.Client, rule.Name))
		}

		if rule.Target == nil {
			continue
		}
		_, ok = clients[rule.Target.Client]
		if !ok {
			return nil, errors.Trace(errors.Errorf("client (%s) not found in rule (%s)", rule.Target.Client, rule.Name))
		}
	}

	rulers := make([]*Ruler, 0)
	for _, l := range cfg.Rules {
		ruler, err := newRuler(l, clients)
		if err != nil {
			return nil, err
		}
		rulers = append(rulers, ruler)
	}
	return rulers, nil
}

func newRuler(rule RuleInfo, clients map[string]ClientInfo) (*Ruler, error) {
	logger := log.With(log.Any("rule", "ruler"), log.Any("name", rule.Name))

	var function *client.Function
	if rule.Function != nil {
		ops := http.NewClientOptions()
		function = &client.Function{
			URL: resolveFunctionAddress(rule.Function.Name),
			CLI: http.NewClient(ops),
		}
	}

	var target client.Client
	if rule.Target != nil {
		var err error
		target, err = NewClient(rule.Name, "target", *rule.Target, clients[rule.Target.Client])
		if err != nil {
			return nil, err
		}

		// source'QOS should be degraded if target'QOS is lower
		if rule.Source.QOS > rule.Target.QOS {
			rule.Source.QOS = rule.Target.QOS
		}
	}

	source, err := NewClient(rule.Name, "source", *rule.Source, clients[rule.Source.Client])
	if err != nil {
		return nil, err
	}

	source.Start(mqtt.NewObserverWrapper(func(pkt *packet.Publish) error {
		data := pkt.Message.Payload
		if function != nil {
			data, err = function.Post(pkt.Message.Payload)
			if err != nil {
				logger.Error("error occured when invoke function in source", log.Any("function", rule.Function.Name), log.Error(err))
				return nil
			}
		}
		if target == nil || len(data) == 0 {
			if rule.Source.QOS == 1 {
				puback := packet.NewPuback()
				puback.ID = pkt.ID
				err := source.SendOrDrop(puback)
				if err != nil {
					logger.Error("error occured when send puback in source", log.Error(err))
				}
			}
			return nil
		}
		pkt.Message.Payload = data
		pkt.Message.Topic = rule.Target.Topic
		err = target.SendOrDrop(pkt)
		if err != nil {
			logger.Error("error occured when send pkt to target in source", log.Error(err))
		}
		return nil
	}, func(*packet.Puback) error {
		return nil
	}, func(err error) {
		logger.Error("error occurs in source", log.Error(err))
	}))

	if target != nil {
		target.Start(mqtt.NewObserverWrapper(func(pkt *packet.Publish) error {
			return nil
		}, func(pkt *packet.Puback) error {
			err := source.SendOrDrop(pkt)
			if err != nil {
				logger.Error("error occured when send pkt to source in target", log.Error(err))
			}
			return nil
		}, func(err error) {
			logger.Error("error occurs in target", log.Error(err))
		}))
	}

	return &Ruler{
		cfg:      rule,
		source:   source,
		target:   target,
		function: function,
		log:      logger,
	}, nil
}

func (l *Ruler) Close() {
	if l.source != nil {
		l.source.Close()
	}
	if l.target != nil {
		l.target.Close()
	}
}
