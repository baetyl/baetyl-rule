package main

import (
	"github.com/baetyl/baetyl-go/v2/utils"
	"time"
)

// Config config of rule
type Config struct {
	Points []Point `yaml:"points" json:"points"`
	Lines  []Line  `yaml:"lines" json:"lines"`
}

// Point point config
type Point struct {
	Name string `yaml:"name" json:"name" validate:"nonzero"`
	Mqtt *Mqtt  `yaml:"mqtt" json:"mqtt" validate:"nonzero"`
}

type Mqtt struct {
	Address              string        `yaml:"address" json:"address" validate:"nonzero"`
	Username             string        `yaml:"username" json:"username"`
	Password             string        `yaml:"password" json:"password"`
	CleanSession         bool          `yaml:"cleansession" json:"cleansession"`
	Timeout              time.Duration `yaml:"timeout" json:"timeout" default:"30s"`
	KeepAlive            time.Duration `yaml:"keepalive" json:"keepalive" default:"30m"`
	MaxReconnectInterval time.Duration `yaml:"maxReconnectInterval" json:"maxReconnectInterval" default:"30m"`
	MaxCacheMessages     int           `yaml:"maxCacheMessages" json:"maxCacheMessages" default:"4194304"`
	utils.Certificate    `yaml:",inline" json:",inline"`
}

type Line struct {
	Name   string    `yaml:"name" json:"name" validate:"nonzero"`
	Source *LineNode `yaml:"source" json:"source" validate:"nonzero" default:"{}"`
	Sink   *LineNode `yaml:"sink" json:"sink"`
	Filter *Filter   `yaml:"filter" json:"filter"`
}

type LineNode struct {
	Point string `yaml:"point" json:"point" default:"baetyl-broker"`
	QOS   int    `yaml:"qos" json:"qos" validate:"min=0, max=1"`
	Topic string `yaml:"topic" json:"topic" validate:"nonzero"`
}

type Filter struct {
	Function string `yaml:"function" json:"function" validate:"nonzero"`
}
