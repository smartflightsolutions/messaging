package messaging

import (
	"cloud.google.com/go/pubsub"
	"log"
)

// GCPubSub Client and topic
type GCPubSub struct {
	pubsubClient *pubsub.Client
	pubsubTopic  *pubsub.Topic
	Options
}

// Options contains project id
type Options struct {
	ProjectID string
	Logger    *log.Logger
}
