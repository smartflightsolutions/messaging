package messaging

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
)

type gcPubSub struct {
	Options
	client *pubsub.Client
}

type PublishOptions struct {
	Topic   string
	Message interface{}
}

func NewGCPubSub(opts Options) PubSubClient {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(300)*time.Second)
	defer cancel()

	pubsubClient, err := pubsub.NewClient(ctx, opts.ProjectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient error: %v", err)
	}

	return &gcPubSub{
		client:  pubsubClient,
		Options: opts,
	}
}

func (ps *gcPubSub) Publish(opts PublishOptions) {
	data, err := json.Marshal(opts.Message)
	if err != nil {
		log.Print(errors.Wrap(err, "Err converting msg: "))
	}

	topic := createTopicIfNotExists(ps, opts.Topic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	if _, err := resp.Get(ctx); err != nil {
		log.Print(errors.Wrap(err, "Err get publish result: "))
	}
}

func createTopicIfNotExists(ps *gcPubSub, topicID string) *pubsub.Topic {
	var err error
	var topic *pubsub.Topic

	topic = ps.client.Topic(topicID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tExist, err := topic.Exists(ctx)
	if err != nil {
		log.Print(errors.Wrap(err, "Err check if topic exist:"))
	}
	if tExist {
		return topic
	}

	topic, err = ps.client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Print(err, errors.Wrap(err, "Create topic err"))
	}

	return topic
}
