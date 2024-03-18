package client

import (
	"io"

	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-rule/v2/config"
)

type Client interface {
	SendOrDrop(pkt *config.TargetMsg) error
	SendPubAck(pkt mqtt.Packet) error
	Start(obs mqtt.Observer) error
	ResetClient(cfg *mqtt.ClientConfig)
	SetReconnectCallback(callback mqtt.ReconnectCallback)
	io.Closer
}
