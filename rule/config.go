package rule

import (
	"encoding/json"

	"github.com/baetyl/baetyl-go/v2/utils"
)

type kind string

// All kinds
const (
	KindMqtt kind = "mqtt"
)

// Config config of rule
type Config struct {
	Clients []ClientInfo `yaml:"clients" json:"clients"`
	Rules   []RuleInfo   `yaml:"rules" json:"rules"`
}

// ClientInfo client info
type ClientInfo struct {
	Name  string                 `yaml:"name" json:"name" validate:"nonzero"`
	Kind  kind                   `yaml:"kind" json:"kind" validate:"nonzero"`
	Value map[string]interface{} `yaml:",inline" json:",inline"`
}

// Parse parse to get real config
func (v *ClientInfo) Parse(in interface{}) error {
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

// ClientRef ref to client
type ClientRef struct {
	Client string `yaml:"client" json:"client" default:"baetyl-broker"`
	QOS    int    `yaml:"qos" json:"qos" validate:"min=0, max=1"`
	Topic  string `yaml:"topic" json:"topic" validate:"nonzero"`
}

// FunctionInfo function info
type FunctionInfo struct {
	Name string `yaml:"name" json:"name" validate:"nonzero"`
}
