package messaging

import "log"

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
	log.Printf("PubSub publishing to topicID %s, msg %v", opts.Topic, opts.Message)
}
