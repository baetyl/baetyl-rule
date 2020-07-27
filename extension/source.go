package extension

import (
	"io"

	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type Source interface {
	SendOrDrop(pkt mqtt.Packet) error
	Start(filter *Filter, sink Sink)
	io.Closer
}

type Filter struct {
	Url string
	Cli *http.Client
}
