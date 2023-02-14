package rule

import (
	"strings"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-rule/v2/client"
)

type SingleClient struct {
	name    string
	client  client.Client
	rulers  map[string]RuleInfo // key: rule name
	subTree *mqtt.Trie
	logger  *log.Logger
}

func (l *SingleClient) Start(clients map[string]*SingleClient, functionClient *http.Client) error {
	var err error
	if l.subTree.Count() == 0 {
		l.client.Start(nil)
		return nil
	}
	l.client.Start(mqtt.NewObserverWrapper(func(pkt *packet.Publish) error {
		data := pkt.Message.Payload
		rulers := l.subTree.Match(pkt.Message.Topic)
		for _, v := range rulers {
			ruleName := v.(string)
			rule := l.rulers[ruleName]
			source := l.client
			target, _ := clients[rule.Target.Client]
			l.logger.Debug("process source pkt", log.Any("topic", pkt.Message.Topic), log.Any("id", pkt.ID))
			if rule.Function != nil {
				l.logger.Debug("call function", log.Any("function", rule.Function.Name))
				data, err = functionClient.Call(rule.Function.Name, pkt.Message.Payload)
				if err != nil {
					l.logger.Error("error occured when invoke function in source", log.Any("function", rule.Function.Name), log.Error(err))
					return nil
				}
			}
			if rule.Target != nil && len(data) != 0 {
				out := mqtt.NewPublish()
				out.ID = pkt.ID
				out.Dup = pkt.Dup
				out.Message = packet.Message{
					Topic:   RegularPubTopic(rule.Source.Topic, pkt.Message.Topic, rule.Target.Topic, rule.Target.Path),
					Payload: data,
					QOS:     pkt.Message.QOS,
					Retain:  pkt.Message.Retain,
				}
				err = target.client.SendOrDrop(rule.Target.Method, out)
				if err != nil {
					l.logger.Error("error occurred when send pkt to target in source", log.Error(err))
				}
				l.logger.Debug("send pkt to target in source", log.Any("pkt", out))
			}
			if pkt.Message.QOS == 1 {
				puback := packet.NewPuback()
				puback.ID = pkt.ID
				err = source.SendPubAck(puback)
				if err != nil {
					l.logger.Error("error occured when send puback in source", log.Error(err))
				}
			}
		}
		return nil
	}, func(*packet.Puback) error {
		return nil
	}, func(err error) {
		l.logger.Error("error occurs in source", log.Error(err))
	}))
	return nil
}

func RegularPubTopic(source, actual, pub, path string) string {
	if path != "" {
		pub = path
	}
	index := strings.Index(pub, "+")
	if index == -1 {
		return pub
	}
	sourceStr := strings.Split(source, "+")
	if len(sourceStr) != 2 {
		return pub
	}
	replace := strings.TrimSuffix(strings.TrimPrefix(actual, sourceStr[0]), sourceStr[1])
	return strings.Replace(pub, "+", replace, 1)
}
