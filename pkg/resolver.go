package pkg

import "fmt"

const (
	BaetylRule      = "baetyl-rule"
	SystemNamespace = "baetyl-edge-system"
	BaetylFunction  = "baetyl-function"
	BaetylBroker    = "baetyl-broker"
)

type Resolver interface {
	// Resolve resolves address
	Resolve(schema, address string) string
}

type resolver struct{}

func NewResolver() Resolver {
	return new(resolver)
}

func (r *resolver) Resolve(schema, address string) string {
	return fmt.Sprintf("%s://%s.%s", schema, BaetylFunction, SystemNamespace)
}
