package messaging

import "time"

type devPubSub struct {
	Options
	topic string
}

func NewDevPubSub(opts Options) PubSubClient {
	return &devPubSub{
		Options: opts,
	}
}

func (ps *devPubSub) Publish(opts PublishOptions) {
	start := time.Now()
	ps.Logger.WithFields(map[string]interface{}{
		"client":   "dev",
		"duration": time.Since(start),
		"topic":    opts.Topic,
		"message":  opts.Message,
	}).Info("Published")
}
