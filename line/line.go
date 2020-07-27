package line

import (
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-rule/extension"
)

type Liner struct {
	cfg      Line
	source   extension.Source
	sink     extension.Sink
	resolver Resolver
	log      *log.Logger
}

func NewLines(cfg Config) ([]*Liner, error) {
	pointers := make(map[string]Point)
	for _, v := range cfg.Points {
		pointers[v.Name] = v
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

	resolver := NewResolver()

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

func newLiner(line Line, pointers map[string]Point, resolver Resolver) (*Liner, error) {
	var filter *extension.Filter
	if line.Filter != nil {
		ops := http.NewClientOptions()
		filter = &extension.Filter{
			Url: resolver.ResolveID(line.Filter.Function),
			Cli: http.NewClient(ops),
		}
	}

	source, err := GetSource(line, pointers[line.Source.Point])
	if err != nil {
		return nil, err
	}

	sink, err := GetSink(line, pointers[line.Sink.Point])
	if err != nil {
		return nil, err
	}

	source.Start(filter, sink)
	sink.Start(source)

	return &Liner{
		cfg:      line,
		source:   source,
		sink:     sink,
		resolver: resolver,
		log:      log.With(log.Any("main", "line"), log.Any("line", line.Name)),
	}, nil
}

func (l *Liner) Close() {
	if l.source != nil {
		l.source.Close()
	}
	if l.sink != nil {
		l.sink.Close()
	}
}
