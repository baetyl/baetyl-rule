package rule

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-rule/v2/client"
	"github.com/baetyl/baetyl-rule/v2/config"
)

type SingleClient struct {
	name    string
	client  client.Client
	rulers  map[string]config.RuleInfo // key: rule name
	subTree *mqtt.Trie
	logger  *log.Logger
}

func (l *SingleClient) Start(clients map[string]*SingleClient, functionClient *http.Client) error {
	var err error
	if l.subTree.Count() == 0 {
		return l.client.Start(nil)
	}
	err = l.client.Start(mqtt.NewObserverWrapper(func(pkt *packet.Publish) error {
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
				pkt.Message.Payload = data
			}
			if rule.Target != nil && len(data) != 0 {
				out := generatePackage(config.KindMqtt, pkt, rule.Source, rule.Target)
				err = target.client.SendOrDrop(out)
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
	return err
}
