package pkg

import (
	"bytes"
	"io"
	"io/ioutil"
	gohttp "net/http"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
)

type Client interface {
	SendOrDrop(pkt mqtt.Packet) error
	Start(filter *Filter, sink Client)
	io.Closer
}

type Filter struct {
	url string
	cli *http.Client
}

func (f *Filter) Post(payload []byte) ([]byte, error) {
	resp, err := f.cli.PostURL(f.url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != gohttp.StatusOK {
		return nil, errors.Errorf("[%d] %s", resp.StatusCode, resp.Status)
	}
	return data, nil
}

func GetClient(lineName string, node LineNode, point PointInfo) (s Client, err error) {
	switch point.Kind {
	case KindMqtt:
		cfg := new(mqtt.ClientConfig)
		err = utils.SetDefaults(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = point.Parse(cfg)
		cfg.ClientID = generateClientID(lineName, point.Name)
		cfg.DisableAutoAck = true
		cfg.Subscriptions = []mqtt.QOSTopic{
			{
				QOS:   uint32(node.QOS),
				Topic: node.Topic,
			},
		}
		s, err = NewMqttExtension(cfg)
	default:
		err = errors.Trace(errors.Errorf("point kind (%s) is not supported"))
	}
	return s, err
}
