package pkg

import (
	"encoding/json"
)

type Kind string

// All kinds
const (
	KindMqtt Kind = "mqtt"
)

// Config config of rule
type Config struct {
	Points []PointInfo `yaml:"points" json:"points"`
	Lines  []LineInfo  `yaml:"lines" json:"lines"`
}

// PointInfo point config
type PointInfo struct {
	Name  string                 `yaml:"name" json:"name" validate:"nonzero"`
	Kind  Kind                   `yaml:"kind" json:"kind" validate:"nonzero"`
	Value map[string]interface{} `yaml:",inline" json:",inline"`
}

// Parse parse to get real config type
func (v *PointInfo) Parse(in interface{}) error {
	data, err := json.Marshal(v.Value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, in)
}

// LineInfo line config
type LineInfo struct {
	Name   string      `yaml:"name" json:"name" validate:"nonzero"`
	Source *LineNode   `yaml:"source" json:"source"`
	Sink   *LineNode   `yaml:"sink" json:"sink"`
	Filter *FilterInfo `yaml:"filter" json:"filter"`
}

// LineNode linenode config
type LineNode struct {
	Point string `yaml:"point" json:"point" default:"baetyl-broker"`
	QOS   int    `yaml:"qos" json:"qos" validate:"min=0, max=1"`
	Topic string `yaml:"topic" json:"topic" validate:"nonzero"`
}

// FilterInfo filter config
type FilterInfo struct {
	Function string `yaml:"function" json:"function" validate:"nonzero"`
}
