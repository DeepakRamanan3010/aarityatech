package main

import (
	"context"

	"github.com/aarityatech/pkg/config"
	"github.com/aarityatech/pkg/telemetry"
	"github.com/spf13/cobra"

	"github.com/aarityatech/tickerfeed/internal/feedserver"
)

const feedServerServiceName = "feedserver"

func cmdServer() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "server",
		Short:   "The tickerfeed websocket server.",
		Aliases: []string{"feedserver"},
	}

	cmd.PersistentFlags().StringP("config", "c", "", "override configuration file")

	cmd.AddCommand(
		cmdServerUp(),
		cmdServerConfigs(),
	)

	return cmd
}

func cmdServerUp() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "up",
		Short:   "Starts the tickerfeed websocket server.",
		Aliases: []string{"start", "run", "serve"},
	}

	cmd.Run = func(cmd *cobra.Command, args []string) {
		cfg := loadServerConfig(cmd)

		cleanup, err := telemetry.Init(cmd.Context(), cfg.Telemetry)
		if err != nil {
			die("failed to initialise telemetry", err)
		}
		defer func() { _ = cleanup(context.Background()) }()

		if err := feedserver.Serve(cmd.Context(), cfg); err != nil {
			die("failed to start server", err)
		}
	}

	return cmd
}

func cmdServerConfigs() *cobra.Command {
	return &cobra.Command{
		Use:   "configs",
		Short: "Show current configuration",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := loadServerConfig(cmd)
			printConf(cmd.Context(), cfg)
		},
	}
}

func loadServerConfig(cmd *cobra.Command) feedserver.Config {
	var cfg feedserver.Config
	if err := config.Load(cmd, feedServerServiceName, &cfg); err != nil {
		die("failed to load configuration", err)
	}

	cfg.Telemetry.ServiceVersion = cmd.Version
	if cfg.Telemetry.ServiceName == "" {
		cfg.Telemetry.ServiceName = feedServerServiceName
	}
	return cfg
}
