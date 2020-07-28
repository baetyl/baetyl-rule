package pkg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolver(t *testing.T) {
	resolver := NewResolver()
	res := resolver.ResolveHost("tcp", "test")
	assert.Equal(t, res, "tcp://test.baetyl-edge-system")

	res = resolver.ResolveHost("http", "test")
	assert.Equal(t, res, "http://test.baetyl-edge-system")
}
