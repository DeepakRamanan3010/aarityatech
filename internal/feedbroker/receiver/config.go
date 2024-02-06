package receiver

import (
	"time"

	"github.com/aarityatech/pkg/udprecv"
)

// Config is the configuration for the udp receiver.
type Config struct {
	udprecv.Config `mapstructure:",squash"`

	Workers int `mapstructure:"workers" default:"1"`

	DiskCache CacheConfig `mapstructure:"cache"`
}

type CacheConfig struct {
	Enable        bool          `mapstructure:"enable" default:"false"`
	CachePath     string        `mapstructure:"path" default:"./cache"`
	FlushInterval time.Duration `mapstructure:"flush_interval" default:"1s"`
}

func (cfg *Config) sanitise() error {
	cfg.MinPacketSize = 36

	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = 1024
	}

	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 3 * time.Second
	}

	cfg.CloneBuffer = false // we don't need to clone the buffer.
	return nil
}
