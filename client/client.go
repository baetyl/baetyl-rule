package client

import (
	"io"

	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type Client interface {
	SendOrDrop(pkt mqtt.Packet) error
	Start(obs mqtt.Observer)
	io.Closer
}
