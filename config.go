package main

import (
	"github.com/baetyl/baetyl-go/v2/utils"
)

// Config config of rule
type Config struct {
	Points []Point `yaml:"points" json:"points" default:"[]"`
	Lines  []Line  `yaml:"lines" json:"lines" default:"[]"`
}

// Point point config
type Point struct {
	Name string `yaml:"name" json:"name" validate:"nonzero"`
	Mqtt *Mqtt  `yaml:"mqtt" json:"mqtt"`
}

type Mqtt struct {
	Address           string `yaml:"address" json:"address" validate:"nonzero"`
	Username          string `yaml:"username" json:"username"`
	Password          string `yaml:"password" json:"password"`
	utils.Certificate `yaml:",inline" json:",inline"`
}

type Line struct {
	Name   string    `yaml:"name" json:"name" validate:"nonzero"`
	Source *LineNode `yaml:"source" json:"source" validate:"nonzero"`
	Sink   *LineNode `yaml:"sink" json:"sink"`
	Filter *Filter   `yaml:"filter" json:"filter"`
}

type LineNode struct {
	Point string `yaml:"point" json:"point" default:"baetyl-broker"`
	QOS   uint32 `yaml:"qos" json:"qos" validate:"min=0, max=1"`
	Topic string `yaml:"topic" json:"topic" validate:"nonzero"`
}

type Filter struct {
	Function string `yaml:"function" json:"function" validate:"nonzero"`
}
