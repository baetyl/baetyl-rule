package rule

import (
	"fmt"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-rule/v2/client"
)

const (
	BaetylRule                = "baetyl-rule"
	BaetylEdgeSystemNamespace = "baetyl-edge-system"
	BaetylFunction            = "baetyl-function"
	BaetylBroker              = "baetyl-broker"
	BaetylBrokerPort          = 1883
)

func NewClient(ruleName, refType string, ref ClientRef, clientInfo ClientInfo) (s client.Client, err error) {
	switch clientInfo.Kind {
	case kindMqtt:
		cfg := new(mqtt.ClientConfig)
		err = clientInfo.Parse(cfg)
		cfg.ClientID = generateClientID(ruleName, refType)
		cfg.DisableAutoAck = true
		cfg.CleanSession = false
		if refType == "source" {
			cfg.Subscriptions = []mqtt.QOSTopic{
				{
					QOS:   uint32(ref.QOS),
					Topic: ref.Topic,
				},
			}
		}
		s, err = client.NewMqttClient(cfg)
	default:
		err = errors.Trace(errors.Errorf("client kind (%s) is not supported", clientInfo.Kind))
	}
	return s, err
}

func getBrokerClient() ClientInfo {
	return ClientInfo{
		Name: BaetylBroker,
		Kind: kindMqtt,
		Value: map[string]interface{}{
			"address": fmt.Sprintf("%s://%s:%d", "tcp", BaetylBroker, BaetylBrokerPort),
		},
	}
}

func generateClientID(rule, name string) string {
	return fmt.Sprintf("%s-%s-%s", BaetylRule, rule, name)
}

func resolveFunctionAddress(function string) string {
	return fmt.Sprintf("http://%s.%s/%s", BaetylFunction, BaetylEdgeSystemNamespace, function)
}
