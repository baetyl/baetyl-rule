package config

import (
	"encoding/json"

	"github.com/baetyl/baetyl-go/v2/utils"
)

type Kind string

// All kinds
const (
	KindMqtt       Kind = "mqtt"
	KinkHTTP       Kind = "http"
	KindHTTPServer Kind = "http-server"
	KindRabbit     Kind = "rabbit-mq"
	KindKafka      Kind = "kafka"
	KindS3         Kind = "s3"
)

const TaskLength = 1024

type TargetMsg struct {
	TargetInfo ClientRef
	Meta       map[string]any
	Data       []byte
	Topic      string
}

// Config config of rule
type Config struct {
	Clients []ClientInfo `yaml:"clients" json:"clients"`
	Rules   []RuleInfo   `yaml:"rules" json:"rules"`
}

// ClientInfo client info
type ClientInfo struct {
	Name  string                 `yaml:"name" json:"name" validate:"nonzero"`
	Kind  Kind                   `yaml:"kind" json:"kind" validate:"nonzero"`
	Value map[string]interface{} `yaml:",inline" json:",inline"`
}

// Parse parse to get real config
func (v *ClientInfo) Parse(in any) error {
	data, err := json.Marshal(v.Value)
	if err != nil {
		return err
	}
	return utils.UnmarshalJSON(data, in)
}

// RuleInfo rule info
type RuleInfo struct {
	Name     string        `yaml:"name" json:"name" validate:"nonzero"`
	Source   *ClientRef    `yaml:"source" json:"source" validate:"nonzero"`
	Target   *ClientRef    `yaml:"target" json:"target"`
	Function *FunctionInfo `yaml:"function" json:"function"`
}

type RabbitMQRef struct {
	Exchange   string `yaml:"exchange" json:"exchange" default:""`
	RoutingKey string `yaml:"routingKey" json:"routingKey" default:""`
}

type HTTPRef struct {
	Path   string `yaml:"path" json:"path" default:""`
	Method string `yaml:"method" json:"method" default:"POST"`
}

type MQTTRef struct {
	QOS   int    `yaml:"qos" json:"qos" default:"0"`
	Topic string `yaml:"topic" json:"topic" default:""`
}

// ClientRef ref to client
type ClientRef struct {
	Client      string `yaml:"client" json:"client" default:"baetyl-broker"`
	MQTTRef     `yaml:",inline" json:",inline"`
	HTTPRef     `yaml:",inline" json:",inline"`
	RabbitMQRef `yaml:",inline" json:",inline"`
}

// FunctionInfo function info
type FunctionInfo struct {
	Name string `yaml:"name" json:"name" validate:"nonzero"`
}
