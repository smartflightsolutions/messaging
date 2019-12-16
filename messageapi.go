package messaging

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
)

var emptyTopicIDAndMsgErr = "topicID and msg are required"
var emptyProjectIDErr = "projectID can not be blank"

// getTopic if topic does not exist, it'll be created and retrieves the topic
func (g *GooglePubSub) getTopic(ctx context.Context, topicID string) error {
	// get topic
	t := g.client.Topic(topicID)

	// check if exist
	tExist, err := t.Exists(ctx)
	if err != nil {
		return errors.Wrap(err, "getTopic could not check:")
	}

	// if topic does not exist on the server, create it
	if !tExist {
		ctxto, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		t, err = g.client.CreateTopic(ctxto, topicID)
		if err != nil {
			return errors.Wrap(err, "getTopic create topic err:")
		}
	}

	g.topic = t
	return nil
}

// getSubscription if subscription does not exist, it'll be created and retrieves the subscription
func (g *GooglePubSub) getSubscription(ctx context.Context, subID string, topicID string) error {
	// get Subscription
	s := g.client.Subscription(subID)

	// check if exist
	sExist, err := s.Exists(ctx)
	if err != nil {
		return errors.Wrap(err, "getSubscription could not check:")
	}

	// if Subscription does not exist on the server, create it
	if !sExist {
		if err := g.getTopic(ctx, topicID); err != nil {
			return errors.Wrap(err, "GetSubscription err:")
		}

		s, err = g.client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
			Topic:       g.topic,
			AckDeadline: 20 * time.Second,
		})
		if err != nil {
			return errors.Wrap(err, "GetSubscription create subscription err:")
		}
	}

	g.subscription = s
	return nil
}

// send a message to google pub sub
func (g *GooglePubSub) send(ctx context.Context, topicID string, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "send err convert msg to data:")
	}

	// create a client
	if err := g.getTopic(ctx, topicID); err != nil {
		return errors.Wrap(err, "send err:")
	}

	ctxto, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	result := g.topic.Publish(ctxto, &pubsub.Message{
		Data: data,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	_, err = result.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "GooglePubSub send err:")
	}

	return nil
}

// receive get/create subscription and send the send to a channel
func (g *GooglePubSub) receive(ctx context.Context, topicID string, subID string, c chan<- ResultMessage) {
	if err := g.getTopic(ctx, topicID); err != nil {
		c <- ResultMessage{Err: errors.Wrap(err, "receive err:")}
		close(c)
	}

	if err := g.getSubscription(ctx, subID, topicID); err != nil {
		c <- ResultMessage{Err: errors.Wrap(err, "receive err:")}
		close(c)
	}

	var mu sync.Mutex
	err := g.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		c <- ResultMessage{Data: msg.Data}
		msg.Ack()
		mu.Lock()
		defer mu.Unlock()
	})

	if err != nil {
		c <- ResultMessage{Err: errors.Wrap(err, "receive err:")}
		close(c)
	}
}

// publish a message to the current message broker
func (m MessageAPI) publish(ctx context.Context, topicID string, msg interface{}) error {
	if topicID == "" || msg == nil {
		return errors.New(emptyTopicIDAndMsgErr)
	}

	return m.API.send(ctx, topicID, msg)
}

// receive send to a channel a message
func (m MessageAPI) receive(ctx context.Context, topicID string, subID string, c chan<- ResultMessage) {
	if topicID == "" || subID == "" {
		c <- ResultMessage{Err: errors.New(emptyTopicIDAndMsgErr)}
		close(c)
	}

	m.API.receive(ctx, topicID, subID, c)
}

// NewGooglePubSub create a client of google pub sub
func NewGooglePubSub(ctx context.Context, projectID string) (*GooglePubSub, error) {
	if projectID == "" {
		return nil, errors.New(emptyProjectIDErr)
	}

	g := &GooglePubSub{projectID: projectID}

	// create client
	client, err := pubsub.NewClient(ctx, g.projectID)
	if err != nil {
		return nil, errors.Wrap(err, "send new client err:")
	}

	g.client = client
	return g, nil
}

// NewMessageAPI create a message broker
func NewMessageAPI(api messageBroker) MessageAPI {
	m := MessageAPI{API: api}

	return m
}

// PublishMessage a message to the message service
func PublishMessage(projectID, topicID string, msg interface{}) error {
	ctx := context.Background()
	g, err := NewGooglePubSub(ctx, projectID)
	if err != nil {
		return errors.Wrap(err, "PublishMessage:")
	}

	m := NewMessageAPI(g)
	return m.publish(ctx, topicID, msg)
}

// ReceiveMessage subcribe to a topic to receive messsages
func ReceiveMessage(projectID string, topicID string, subID string, c chan<- ResultMessage) {
	ctx := context.Background()
	g, err := NewGooglePubSub(ctx, projectID)
	if err != nil {
		c <- ResultMessage{Err: errors.Wrap(err, "ReceiveMessage:")}
		close(c)
	}

	m := NewMessageAPI(g)
	m.receive(ctx, topicID, subID, c)
}
