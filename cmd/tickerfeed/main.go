package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aarityatech/pkg/log"
	"github.com/spf13/cobra"
)

var (
	Version = "N/A"
	Commit  = "N/A"
	BuiltOn = "N/A"

	rootCmd = &cobra.Command{
		Use:               "tickerfeed",
		Short:             "Live market data feed system.",
		Version:           fmt.Sprintf("%s\ncommit: %s\nbuild date: %s", Version, Commit, BuiltOn),
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var logLevel, logMode string
	flags := rootCmd.PersistentFlags()
	flags.StringVarP(&logLevel, "log-level", "L", "info", "log level")
	flags.StringVarP(&logMode, "log-mode", "M", "text", "log mode (off, dev, prod)")

	var cleanupLogger = func() {}
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		cleanup, err := log.Setup(logMode, logLevel)
		if err != nil {
			die("failed to setup logger", err)
		}
		cleanupLogger = cleanup
	}

	rootCmd.PersistentPostRun = func(cmd *cobra.Command, args []string) {
		cleanupLogger()
	}

	rootCmd.PersistentFlags().StringP("config", "c", "", "override configuration file")
	rootCmd.AddCommand(
		cmdBroker(),
		cmdServer(),
	)

	_ = rootCmd.ExecuteContext(ctx)
}

func die(msg string, err error) {
	log.Fatal(context.Background(), msg, log.Err(err))
	os.Exit(1)
}
