package main

import (
	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-rule/line"
)

func main() {
	context.Run(func(ctx context.Context) error {
		var cfg line.Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}

		lines, err := line.NewLines(cfg)
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
