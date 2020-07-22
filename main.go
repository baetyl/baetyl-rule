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

		lines, err := NewLines(cfg)
		if err != nil {
			return err
		}
		defer func() {
			for _, liner := range lines {
				liner.close()
			}
		}()

		ctx.Wait()
		return nil
	})
}
