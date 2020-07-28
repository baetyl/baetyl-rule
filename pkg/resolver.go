package pkg

import "fmt"

type Resolver interface {
	// Resolve resolves host
	ResolveHost(schema, host string) string
}

type resolver struct{}

func NewResolver() Resolver {
	return new(resolver)
}

func (r *resolver) ResolveHost(schema, host string) string {
	return fmt.Sprintf("%s://%s.%s", schema, host, SystemNamespace)
}
