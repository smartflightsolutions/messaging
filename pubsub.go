package messaging

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"log"
	"os"
	"time"
)

func optionsWithDefault(opts *Options) {
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stdout, "pubsub: ", log.Lshortfile)
	}
}

// New create a client and return GCPubSub
func New(opt Options) (*GCPubSub, error) {
	optionsWithDefault(&opt)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pubsubClient, err := pubsub.NewClient(ctx, opt.ProjectID)
	if err != nil {
		return nil, errors.Wrap(err, "Err on new PubSub")
	}

	return &GCPubSub{
		pubsubClient: pubsubClient,
		Options:      opt,
	}, nil
}

// Topic set topic in GCPubSub
func (m *GCPubSub) Topic(id string) *GCPubSub {
	var err error
	var topic *pubsub.Topic

	topic = m.pubsubClient.Topic(id)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tExist, err := topic.Exists(ctx)
	if err != nil {
		m.Logger.Print(errors.Wrap(err, "Err check if topic exist:"))
	}
	if tExist {
		m.pubsubTopic = topic
	} else {
		topic, err = m.pubsubClient.CreateTopic(ctx, id)
		if err != nil {
			m.Logger.Print(err, errors.Wrap(err, "Create topic err"))
		}
		m.pubsubTopic = topic
	}

	return m
}

// Publish create an event into the topic
func (m *GCPubSub) Publish(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		m.Logger.Print(errors.Wrap(err, "Err converting msg: "))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := m.pubsubTopic.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	if _, err := resp.Get(ctx); err != nil {
		m.Logger.Print(errors.Wrap(err, "Err get publish result: "))
	}
}
