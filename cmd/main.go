package main

import (
	"fmt"

	"github.com/baetyl/baetyl-go/v2/context"

	"github.com/baetyl/baetyl-rule/v2/rule"
)

func main() {
	context.Run(func(ctx context.Context) error {
		if err := ctx.CheckSystemCert(); err != nil {
			return err
		}

		var cfg rule.Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}

		// baetyl-broker client is the mqtt broker in edge
		systemCert := ctx.SystemConfig().Certificate
		cfg.Clients = append(cfg.Clients, rule.ClientInfo{
			Name: "baetyl-broker",
			Kind: rule.KindMqtt,
			Value: map[string]interface{}{
				"address": fmt.Sprintf("%s://%s:%s", "ssl", context.BrokerHost(), context.BrokerPort()),
				"ca":      systemCert.CA,
				"cert":    systemCert.Cert,
				"key":     systemCert.Key,
			},
		})

		function, err := ctx.NewFunctionHttpClient()
		if err != nil {
			return err
		}

		rulers, err := rule.NewRulers(ctx, cfg, function)
		defer rulers.Close()
		if err != nil {
			return err
		}

		ctx.Wait()
		return nil
	})
}
