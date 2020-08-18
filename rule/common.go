package rule

import (
	"fmt"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-rule/v2/client"
)

func NewClient(ruleName, refType string, ref ClientRef, clientInfo ClientInfo) (s client.Client, err error) {
	switch clientInfo.Kind {
	case KindMqtt:
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

func generateClientID(rule, name string) string {
	return fmt.Sprintf("%s-%s-%s", "baetyl-rule", rule, name)
}
