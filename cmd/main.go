package main

import (
	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-rule/v2/rule"
)

func main() {
	context.Run(func(ctx context.Context) error {
		var cfg rule.Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}

		rulers, err := rule.NewRulers(cfg)
		if err != nil {
			return err
		}
		defer func() {
			for _, ruler := range rulers {
				ruler.Close()
			}
		}()

		ctx.Wait()
		return nil
	})
}
