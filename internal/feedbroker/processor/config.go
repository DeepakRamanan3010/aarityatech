package processor

import "time"

type Config struct {
	DerivedFuture bool          `mapstructure:"derived_future" default:"false"`
	RunInterval   time.Duration `mapstructure:"run_interval" default:"1m"`
}
