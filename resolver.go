package main

import "fmt"

const (
	BaetylRule      = "baetyl-rule"
	SystemNamespace = "baetyl-edge-system"
	BaetylFunction  = "baetyl-function"
)

type Resolver interface {
	// ResolveID resolves name to address
	ResolveID(name string) string
}

type resolver struct{}

func NewResolver() Resolver {
	return &resolver{}
}

func (r *resolver) ResolveID(name string) string {
	return fmt.Sprintf("http://%s.%s/%s", BaetylFunction, SystemNamespace, name)
}
