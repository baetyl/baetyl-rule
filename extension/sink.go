package extension

import (
	"io"

	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type Sink interface {
	SendOrDrop(pkt mqtt.Packet) error
	Start(source Source)
	io.Closer
}
