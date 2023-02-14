package rule

import (
	"fmt"

	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-rule/v2/client"
)

func NewClient(ctx context.Context, clientDetail *ClientDetail) (s client.Client, err error) {
	switch clientDetail.Info.Kind {
	case KindMqtt:
		cfg := new(mqtt.ClientConfig)
		err = clientDetail.Info.Parse(cfg)
		cfg.ClientID = generateClientID(clientDetail.Name)
		cfg.DisableAutoAck = true
		cfg.CleanSession = true
		cfg.Subscriptions = clientDetail.Subscription
		s, err = client.NewMqttClient(cfg)
	case KinkHTTP:
		cfg := new(mqtt.ClientConfig)
		err = clientDetail.Info.Parse(cfg)
		s, err = client.NewHTTPClient(ctx, cfg)
	default:
		err = errors.Trace(errors.Errorf("client kind (%s) is not supported", clientDetail.Info.Kind))
	}
	return s, err
}

func generateClientID(name string) string {
	return fmt.Sprintf("%s-%s", "baetyl-rule", name)
}
