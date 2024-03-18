package rule

import (
	"fmt"
	"strings"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-rule/v2/client"
	"github.com/baetyl/baetyl-rule/v2/config"
)

func NewClient(ctx context.Context, clientDetail *ClientDetail) (s client.Client, err error) {
	switch clientDetail.Info.Kind {
	case config.KindMqtt:
		cfg := new(mqtt.ClientConfig)
		err = clientDetail.Info.Parse(cfg)
		cfg.ClientID = generateClientID(ctx.AppName(), clientDetail.Name)
		cfg.DisableAutoAck = true
		cfg.CleanSession = true
		cfg.Subscriptions = clientDetail.Subscription
		s, err = client.NewMqttClient(cfg)
	case config.KinkHTTP:
		cfg := new(client.HTTPClientCfg)
		err = clientDetail.Info.Parse(cfg)
		s, err = client.NewHTTPClient(ctx, cfg)
	case config.KindRabbit:
		cfg := new(client.RabbitClientCfg)
		err = clientDetail.Info.Parse(cfg)
		s, err = client.NewRabbitClient(ctx, cfg)
	case config.KindS3:
		cfg := new(client.S3ClientCfg)
		err = clientDetail.Info.Parse(cfg)
		s, err = client.NewS3Client(ctx, cfg)
	case config.KindKafka:
		cfg := new(client.KafkaClientCfg)
		err = clientDetail.Info.Parse(cfg)
		s, err = client.NewKafkaClient(ctx, cfg)
	default:
		err = errors.Trace(errors.Errorf("client kind (%s) is not supported", clientDetail.Info.Kind))
	}
	return s, err
}

func generateClientID(appName, name string) string {
	return fmt.Sprintf("%s-%s", appName, name)
}

func generatePackage(k config.Kind, pkt any, source *config.ClientRef, target *config.ClientRef) *config.TargetMsg {
	msg := &config.TargetMsg{
		TargetInfo: *target,
		Meta:       map[string]any{},
	}
	switch k {
	case config.KindMqtt:
		origin := pkt.(*packet.Publish)
		msg.Data = origin.Message.Payload
		msg.Topic = RegularPubTopic(source.Topic, origin.Message.Topic, target.Topic, target.Path)
		msg.Meta["ID"] = origin.ID
		msg.Meta["Dup"] = origin.Dup
		msg.Meta["QoS"] = origin.Message.QOS
		msg.Meta["Retain"] = origin.Message.Retain
	case config.KinkHTTP:
		origin := pkt.([]byte)
		msg.Data = origin
		msg.Topic = RegularPubTopic("", "", target.Topic, target.Path)
	}
	return msg
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
