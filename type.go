package messaging

import (
	"cloud.google.com/go/pubsub"
	"context"
)

// messageBroker provides an interface so we can swap out the
// implementation of SendEmail under tests.
type messageBroker interface {
	send(ctx context.Context, topicID string, msg interface{}) error
	receive(ctx context.Context, topicID string, subID string, c chan<- ResultMessage)
}

// GooglePubSub message broker
type GooglePubSub struct {
	projectID    string
	client       *pubsub.Client
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
}

// MessageAPI standard message api
type MessageAPI struct {
	API messageBroker
}

// ResultMessage type to communicate through a channel
type ResultMessage struct {
	Data []byte
	Err  error
}
