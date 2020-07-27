package pkg

import (
	"fmt"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
)

type Liner struct {
	cfg      LineInfo
	cli1     Client
	cli2     Client
	resolver Resolver
}

func NewLines(cfg Config) ([]*Liner, error) {
	resolver := NewResolver()

	pointers := make(map[string]PointInfo)
	for _, v := range cfg.Points {
		pointers[v.Name] = v
	}

	pointers[BaetylBroker] = PointInfo{
		Name: BaetylBroker,
		Kind: KindMqtt,
		Value: map[string]interface{}{
			"address": resolver.Resolve("tcp", BaetylBroker),
		},
	}

	for _, line := range cfg.Lines {
		if line.Source != nil {
			_, ok := pointers[line.Source.Point]
			if !ok {
				return nil, errors.Trace(errors.Errorf("point (%s) not found in line (%s)", line.Source.Point, line.Name))
			}
		}

		if line.Sink != nil {
			_, ok := pointers[line.Sink.Point]
			if !ok {
				return nil, errors.Trace(errors.Errorf("point (%s) not found in line (%s)", line.Sink.Point, line.Name))
			}
		}
	}

	liners := make([]*Liner, 0)
	for _, l := range cfg.Lines {
		liner, err := newLiner(l, pointers, resolver)
		if err != nil {
			return nil, err
		}
		liners = append(liners, liner)
	}
	return liners, nil
}

func newLiner(line LineInfo, pointers map[string]PointInfo, resolver Resolver) (*Liner, error) {
	var filter *Filter
	if line.Filter != nil {
		ops := http.NewClientOptions()
		filter = &Filter{
			url: fmt.Sprintf("%s/%s", resolver.Resolve("http", line.Filter.Function), line.Filter.Function),
			cli: http.NewClient(ops),
		}
	}

	if line.Source.QOS > line.Sink.QOS {
		line.Source.QOS = line.Sink.QOS
	}
	cli1, err := GetClient(line.Name, *line.Source, pointers[line.Source.Point])
	if err != nil {
		return nil, err
	}

	cli2, err := GetClient(line.Name, *line.Sink, pointers[line.Sink.Point])
	if err != nil {
		return nil, err
	}

	cli1.Start(filter, cli2)
	cli2.Start(nil, cli1)

	return &Liner{
		cfg:      line,
		cli1:     cli1,
		cli2:     cli2,
		resolver: resolver,
	}, nil
}

func (l *Liner) Close() {
	if l.cli1 != nil {
		l.cli1.Close()
	}
	if l.cli2 != nil {
		l.cli2.Close()
	}
}
