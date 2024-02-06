package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/aarityatech/pkg/config"
	"github.com/aarityatech/pkg/telemetry"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"

	"github.com/aarityatech/tickerfeed/internal/feedbroker"
)

const feedBrokerServiceName = "feedbroker"

func cmdBroker() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "broker",
		Short:   "The tickerfeed UDP broker",
		Aliases: []string{"feedbroker"},
	}

	cmd.AddCommand(
		cmdBrokerUp(),
		cmdBrokerConfigs(),
	)

	return cmd
}

func cmdBrokerUp() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "up",
		Short:   "Start tickerfeed UDP broker",
		Aliases: []string{"start", "run", "serve"},
	}

	cmd.Run = func(cmd *cobra.Command, args []string) {
		cfg := loadBrokerConfig(cmd)

		cleanup, err := telemetry.Init(cmd.Context(), cfg.Telemetry)
		if err != nil {
			die("failed to initialise telemetry", err)
		}
		defer func() { _ = cleanup(context.Background()) }()

		if err := feedbroker.Serve(cmd.Context(), cfg); err != nil {
			die("feed broker exited with error", err)
		}
	}

	return cmd
}

func cmdBrokerConfigs() *cobra.Command {
	return &cobra.Command{
		Use:   "configs",
		Short: "Show current configuration",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := loadBrokerConfig(cmd)
			printConf(cmd.Context(), cfg)
		},
	}
}

func loadBrokerConfig(cmd *cobra.Command) feedbroker.Config {
	var cfg feedbroker.Config
	if err := config.Load(cmd, feedBrokerServiceName, &cfg); err != nil {
		die("failed to load configuration", err)
	}

	cfg.Telemetry.ServiceVersion = cmd.Version
	if cfg.Telemetry.ServiceName == "" {
		cfg.Telemetry.ServiceName = feedBrokerServiceName
	}
	return cfg
}

func printConf(ctx context.Context, cfg interface{}) {
	m := map[string]any{}
	if err := mapstructure.Decode(cfg, &m); err != nil {
		die("failed to decode configuration", err)
	}
	_ = json.NewEncoder(os.Stdout).Encode(m)
}
