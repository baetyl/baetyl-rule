package client

import (
	"context"
	"fmt"

	gcontext "github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/wagslane/go-rabbitmq"

	"github.com/baetyl/baetyl-rule/v2/config"
)

type RabbitClientCfg struct {
	Address  string `yaml:"address" json:"address"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

type RabbitClient struct {
	cfg    *RabbitClientCfg
	conn   *rabbitmq.Conn
	pub    *rabbitmq.Publisher
	tasks  chan *config.TargetMsg
	ctx    context.Context
	cancel context.CancelFunc
	logger *log.Logger
}

func NewRabbitClient(_ gcontext.Context, cfg *RabbitClientCfg) (Client, error) {
	var err error
	var conn *rabbitmq.Conn
	if cfg.Username != "" && cfg.Password != "" {
		conn, err = rabbitmq.NewConn(
			fmt.Sprintf("amqp://%s:%s@%s", cfg.Username, cfg.Password, cfg.Address),
			rabbitmq.WithConnectionOptionsLogging,
		)
	} else {
		conn, err = rabbitmq.NewConn(
			fmt.Sprintf("amqp://%s", cfg.Address),
			rabbitmq.WithConnectionOptionsLogging,
		)
	}
	if err != nil {
		return nil, err
	}
	pub, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
	)
	if err != nil {
		return nil, err
	}
	ctxCancel, cancel := context.WithCancel(context.Background())
	return &RabbitClient{
		conn:   conn,
		cfg:    cfg,
		pub:    pub,
		ctx:    ctxCancel,
		cancel: cancel,
		tasks:  make(chan *config.TargetMsg, config.TaskLength),
		logger: log.With(log.Any("client", "rabbit-mq")),
	}, nil
}

func (r *RabbitClient) SendOrDrop(pkt *config.TargetMsg) error {
	select {
	case <-r.ctx.Done():
		return errors.New("rabbit mq has exited")
	case r.tasks <- pkt:
		return nil
	}
}

func (r *RabbitClient) SendPubAck(_ mqtt.Packet) error {
	return nil
}

func (r *RabbitClient) Start(_ mqtt.Observer) error {
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case task := <-r.tasks:
				go r.RabbitSend(task)
			}
		}
	}()
	return nil
}

func (r *RabbitClient) RabbitSend(task *config.TargetMsg) {
	err := r.pub.Publish(
		task.Data,
		[]string{task.TargetInfo.RoutingKey},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsExchange(task.TargetInfo.Exchange),
	)
	if err != nil {
		r.logger.Error("failed to publish rabbit data", log.Error(err))
	}
}

func (r *RabbitClient) ResetClient(_ *mqtt.ClientConfig) {}

func (r *RabbitClient) SetReconnectCallback(_ mqtt.ReconnectCallback) {}

// Close closes client
func (r *RabbitClient) Close() error {
	r.cancel()
	r.pub.Close()
	return r.conn.Close()
}
