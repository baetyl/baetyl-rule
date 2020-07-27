package pkg

import "fmt"

func generateClientID(line, point string) string {
	return fmt.Sprintf("%s-%s-%s", BaetylRule, line, point)
}
