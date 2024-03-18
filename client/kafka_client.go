package client

import (
	"context"
	"crypto/tls"
	"time"

	gcontext "github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/baetyl/baetyl-rule/v2/config"
)

type KafkaClientCfg struct {
	Address           []string `yaml:"address" json:"address"`
	SASLType          string   `yaml:"saslType" json:"saslType" default:""`
	Username          string   `yaml:"username" json:"username"`
	Password          string   `yaml:"password" json:"password"`
	utils.Certificate `yaml:",inline" json:",inline"`
}

type KafkaClient struct {
	cli     *kafka.Dialer
	writer  *kafka.Writer
	address []string
	tasks   chan *config.TargetMsg
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *log.Logger
}

func NewKafkaClient(_ gcontext.Context, cfg *KafkaClientCfg) (Client, error) {
	var tlsCfg *tls.Config
	var err error
	if cfg.CA != "" && cfg.Cert != "" && cfg.Key != "" {
		tlsCfg, err = utils.NewTLSConfigClient(utils.Certificate{
			CA:                 cfg.CA,
			Cert:               cfg.Cert,
			Key:                cfg.Key,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		})
		if err != nil {
			return nil, err
		}
	}
	cli := &kafka.Dialer{
		Timeout:   20 * time.Second,
		DualStack: true,
		TLS:       tlsCfg,
	}

	switch cfg.SASLType {
	case "plain":
		cli.SASLMechanism = plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	case "scram256":
		cli.SASLMechanism, err = scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
		if err != nil {
			return nil, err
		}
	case "scram512":
		cli.SASLMechanism, err = scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
		if err != nil {
			return nil, err
		}
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.Address,
		Dialer:   cli,
		Balancer: &kafka.Hash{},
	})
	ctxCancel, cancel := context.WithCancel(context.Background())
	return &KafkaClient{
		cli:     cli,
		writer:  w,
		address: cfg.Address,
		tasks:   make(chan *config.TargetMsg, config.TaskLength),
		ctx:     ctxCancel,
		cancel:  cancel,
		logger:  log.With(log.Any("client", "http")),
	}, nil
}

func (k *KafkaClient) SendOrDrop(pkt *config.TargetMsg) error {
	select {
	case <-k.ctx.Done():
		return errors.New("ctx done")
	case k.tasks <- pkt:
		return nil
	}
}

func (k *KafkaClient) SendPubAck(_ mqtt.Packet) error {
	return nil
}

func (k *KafkaClient) Start(_ mqtt.Observer) error {
	go func() {
		for {
			select {
			case <-k.ctx.Done():
				return
			case task := <-k.tasks:
				go k.KafkaSend(task)
			}
		}
	}()
	return nil
}

func (k *KafkaClient) KafkaSend(task *config.TargetMsg) {
	err := k.writer.WriteMessages(k.ctx, kafka.Message{
		Topic: task.TargetInfo.Topic,
		Value: task.Data,
	})
	if err != nil {
		k.logger.Error("failed to write kafka msg", log.Error(err))
	}
}

func (k *KafkaClient) ResetClient(_ *mqtt.ClientConfig) {}

func (k *KafkaClient) SetReconnectCallback(_ mqtt.ReconnectCallback) {}

// Close closes client
func (k *KafkaClient) Close() error {
	k.cancel()
	return k.writer.Close()
}
