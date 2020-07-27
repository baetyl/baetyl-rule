package main

import (
	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-rule/pkg"
)

func main() {
	context.Run(func(ctx context.Context) error {
		var cfg pkg.Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}

		lines, err := pkg.NewLines(cfg)
		if err != nil {
			return err
		}
		defer func() {
			for _, line := range lines {
				line.Close()
			}
		}()

		ctx.Wait()
		return nil
	})
}
