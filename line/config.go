package line

import "github.com/mitchellh/mapstructure"

type Kind string

// All kinds
const (
	KindMqtt Kind = "mqtt"
)

// Config config of rule
type Config struct {
	Points []Point `yaml:"points" json:"points"`
	Lines  []Line  `yaml:"lines" json:"lines"`
}

// Point point config
type Point struct {
	Name  string                 `yaml:"name" json:"name" validate:"nonzero"`
	Kind  Kind                   `yaml:"kind" json:"kind" validate:"nonzero"`
	Value map[string]interface{} `yaml:",inline" json:",inline"`
}

// Parse parse to get real config type
func (v *Point) Parse(in interface{}) error {
	return mapstructure.Decode(v.Value, in)
}

// Line line config
type Line struct {
	Name   string    `yaml:"name" json:"name" validate:"nonzero"`
	Source *LineNode `yaml:"source" json:"source"`
	Sink   *LineNode `yaml:"sink" json:"sink"`
	Filter *Filter   `yaml:"filter" json:"filter"`
}

// LineNode linenode config
type LineNode struct {
	Point string `yaml:"point" json:"point" default:"baetyl-broker"`
	QOS   int    `yaml:"qos" json:"qos" validate:"min=0, max=1"`
	Topic string `yaml:"topic" json:"topic" validate:"nonzero"`
}

// Filter filter config
type Filter struct {
	Function string `yaml:"function" json:"function" validate:"nonzero"`
}
