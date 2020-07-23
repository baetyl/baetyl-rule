package main

import (
	"github.com/baetyl/baetyl-go/v2/context"
)

func main() {
	context.Run(func(ctx context.Context) error {
		var cfg Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}

		lines, err := NewLines(cfg, NewResolver())
		if err != nil {
			return err
		}
		defer func() {
			for _, line := range lines {
				line.close()
			}
		}()

		ctx.Wait()
		return nil
	})
}
