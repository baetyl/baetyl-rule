package client

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
)

type HTTPClient struct {
	cli     *http.Client
	address string
}

func NewHTTPClient(ctx context.Context, cfg *mqtt.ClientConfig) (Client, error) {
	options := http.NewClientOptions()
	options.Address = cfg.Address
	if strings.HasPrefix(cfg.Address, "https") {
		var tlsCfg *tls.Config
		var err error
		if cfg.CA == "" || cfg.Cert == "" || cfg.Key == "" {
			cert := ctx.SystemConfig().Certificate
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
	return &HTTPClient{
		cli:     cli,
		address: cfg.Address,
	}, nil
}

func (h *HTTPClient) SendOrDrop(method string, pkt *mqtt.Publish) error {
	header := map[string]string{"Content-Type": "application/json"}
	res, err := h.cli.SendUrl(strings.ToUpper(method), fmt.Sprintf("%s%s", h.address, pkt.Message.Topic), bytes.NewReader(pkt.Message.Payload), header)
	if err != nil {
		return errors.Trace(err)
	}
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}
	return nil
}

func (h *HTTPClient) SendPubAck(pkt mqtt.Packet) error {
	return nil
}

func (h *HTTPClient) Start(obs mqtt.Observer) {
	return
}

func (h *HTTPClient) ResetClient(cfg *mqtt.ClientConfig) {
	return
}

func (h *HTTPClient) SetReconnectCallback(callback mqtt.ReconnectCallback) {
	return
}

// Close closes client
func (h *HTTPClient) Close() error {
	return nil
}
