package client

import (
	"io"

	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type Client interface {
	SendOrDrop(method string, pkt *mqtt.Publish) error
	SendPubAck(pkt mqtt.Packet) error
	Start(obs mqtt.Observer)
	ResetClient(cfg *mqtt.ClientConfig)
	SetReconnectCallback(callback mqtt.ReconnectCallback)
	io.Closer
}
