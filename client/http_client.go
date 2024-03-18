package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	http2 "net/http"
	"strings"

	gcontext "github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"

	"github.com/baetyl/baetyl-rule/v2/config"
)

type HTTPClientCfg struct {
	Address           string `yaml:"address" json:"address"`
	utils.Certificate `yaml:",inline" json:",inline"`
}

type HTTPClient struct {
	cli     *http.Client
	address string
	tasks   chan *config.TargetMsg
	cancel  context.CancelFunc
	ctx     context.Context
	logger  *log.Logger
}

func NewHTTPClient(gctx gcontext.Context, cfg *HTTPClientCfg) (Client, error) {
	options := http.NewClientOptions()
	options.Address = cfg.Address
	if strings.HasPrefix(cfg.Address, "https") {
		var tlsCfg *tls.Config
		var err error
		if cfg.CA == "" || cfg.Cert == "" || cfg.Key == "" {
			cert := gctx.SystemConfig().Certificate
			cert.InsecureSkipVerify = true
			tlsCfg, err = utils.NewTLSConfigClient(cert)
		} else {
			tlsCfg, err = utils.NewTLSConfigClient(utils.Certificate{
				CA:                 cfg.CA,
				Cert:               cfg.Cert,
				Key:                cfg.Key,
				InsecureSkipVerify: cfg.InsecureSkipVerify,
			})
		}
		if err != nil {
			return nil, err
		}
		options.TLSConfig = tlsCfg
	}
	cli := http.NewClient(options)
	ctx, cancel := context.WithCancel(context.Background())
	return &HTTPClient{
		cli:     cli,
		address: cfg.Address,
		cancel:  cancel,
		ctx:     ctx,
		tasks:   make(chan *config.TargetMsg, config.TaskLength),
		logger:  log.With(log.Any("client", "http")),
	}, nil
}

func (h *HTTPClient) SendOrDrop(pkt *config.TargetMsg) error {
	select {
	case <-h.ctx.Done():
		return errors.New("ctx done")
	case h.tasks <- pkt:
		return nil
	}
}

func (h *HTTPClient) SendPubAck(_ mqtt.Packet) error {
	return nil
}

func (h *HTTPClient) Start(_ mqtt.Observer) error {
	go func() {
		for {
			select {
			case <-h.ctx.Done():
				return
			case task := <-h.tasks:
				go h.HTTPSend(task)
			}
		}
	}()
	return nil
}

func (h *HTTPClient) HTTPSend(task *config.TargetMsg) {
	header := map[string]string{"Content-Type": "application/json"}
	res, err := h.cli.SendUrl(strings.ToUpper(task.TargetInfo.Method), fmt.Sprintf("%s%s", h.address, task.Topic), bytes.NewReader(task.Data), header)
	if err != nil {
		h.logger.Error("failed to send http", log.Error(err))
	}
	if res.StatusCode < http2.StatusOK || res.StatusCode > http2.StatusAlreadyReported {
		h.logger.Error("failed to get 200 code", log.Any("status", res.Status))
	}
	h.logger.Debug("HTTP Send msg", log.Any("topic", task.Topic))
}

func (h *HTTPClient) ResetClient(_ *mqtt.ClientConfig) {}

func (h *HTTPClient) SetReconnectCallback(_ mqtt.ReconnectCallback) {}

// Close closes client
func (h *HTTPClient) Close() error {
	h.cancel()
	return nil
}
