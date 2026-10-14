//go:generate ../../../tools/readme_config_includer/generator
package redis_sub

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/redis/go-redis/v9"
)

//go:embed sample.conf
var sampleConfig string

type RedisSub struct {
	Server     string          `toml:"server"`
	Channels   []string        `toml:"channels"`
	DataFormat string          `toml:"data_format"`
	Password   string          `toml:"password"`
	DB         int             `toml:"db"`
	Log        telegraf.Logger `toml:"-"`

	client *redis.Client
	cancel context.CancelFunc
	parser telegraf.Parser
}

func (r *RedisSub) Gather(accumulator telegraf.Accumulator) error {
	return nil
}

func (*RedisSub) SampleConfig() string {
	return sampleConfig
}

func (*RedisSub) Description() string {
	return "Reads metrics from Redis Pub/Sub channels continuously"
}

// Init sets up parser
func (r *RedisSub) Init() error {
	if r.Server == "" {
		r.Server = "localhost:6379"
	}

	if len(r.Channels) == 0 {
		return fmt.Errorf("no channels configured")
	}
	return nil
}

// SetParser attaches Telegraf parser
func (r *RedisSub) SetParser(parser telegraf.Parser) {
	r.parser = parser
}

// Start begins reading from Redis Pub/Sub
func (r *RedisSub) Start(acc telegraf.Accumulator) error {
	if r.parser == nil {
		r.Log.Error("parser is nil")
		return fmt.Errorf("parser not initialized")
	}

	r.Log.Error("Client initialized")

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.client = redis.NewClient(&redis.Options{
		Addr: r.Server,
	})

	go func() {
		defer r.Log.Info("Redis subscriber stopped")

		for {
			// Exit if context canceled
			if ctx.Err() != nil {
				return
			}

			// (Re)connect and subscribe
			r.Log.Infof("Connecting to Redis at %s", r.Server)
			r.client = redis.NewClient(&redis.Options{
				Addr:     r.Server,
				Password: r.Password,
				DB:       r.DB,
			})

			// Test the connection
			if _, err := r.client.Ping(ctx).Result(); err != nil {
				r.Log.Errorf("Failed to connect to Redis: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			sub := r.client.Subscribe(ctx, r.Channels...)
			r.Log.Infof("Subscribed to channels: %v", r.Channels)
			ch := sub.Channel()

			// Listen for messages until subscription closes or context cancels
			for {
				select {
				case <-ctx.Done():
					_ = sub.Close()
					return

				case msg, ok := <-ch:
					if !ok {
						r.Log.Warn("Redis subscription closed, reconnecting...")
						_ = sub.Close()
						time.Sleep(5 * time.Second)
						break
					}

					// Parse and send to accumulator
					metrics, err := r.parser.Parse([]byte(msg.Payload))
					if err != nil {
						acc.AddError(fmt.Errorf("parse error from %q: %v", msg.Channel, err))
						continue
					}

					for _, m := range metrics {
						acc.AddMetric(m)
					}
				}
			}
		}
	}()

	return nil
}

func (r *RedisSub) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	if r.client != nil {
		err := r.client.Close()
		if err != nil {
			r.Log.Info("Redis Pub/Sub stopped with error")
			return
		}
	}
	r.Log.Info("Redis Pub/Sub stopped")
}

func init() {
	inputs.Add("redis_sub", func() telegraf.Input {
		return &RedisSub{}
	})
}
