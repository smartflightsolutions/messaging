package messaging

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"log"
	"testing"
)

// FakeMessageBroker testing messaging
type FakeMessageBroker struct {
	mock.Mock
}

func (mock *FakeMessageBroker) send(ctx context.Context, topicID string, msg interface{}) error {
	args := mock.Called(ctx, topicID, msg)
	return args.Error(0)
}

func (mock *FakeMessageBroker) receive(ctx context.Context, topicID string, subID string, c chan<- ResultMessage) {
	log.Println("receive method to meet the interface")
}

func NewFakeMessageBroker(api messageBroker) MessageAPI {
	service := MessageAPI{API: api}

	return service
}

func TestMessageBroker_Send(t *testing.T) {
	ctx := context.Background()
	mb := &FakeMessageBroker{}
	mb.On("send", ctx, "topic", "message").Return(nil)

	service := NewFakeMessageBroker(mb)
	err := service.publish(ctx, "topic", "message")
	assert.NoError(t, err)
	mb.AssertExpectations(t)
}

func TestMessageBroker_Send_ErrorEmptyTopic(t *testing.T) {
	ctx := context.Background()
	mb := &FakeMessageBroker{}
	mb.On("send", ctx, "", "message").Return(nil)

	service := NewFakeMessageBroker(mb)
	err := service.publish(ctx, "", "message")
	assert.Error(t, err)
	assert.Errorf(t, err, emptyTopicIDAndMsgErr)
}

func TestMessageBroker_Send_ErrorEmptyMessage(t *testing.T) {
	ctx := context.Background()
	mb := &FakeMessageBroker{}
	mb.On("send", ctx, "topic", nil).Return(nil)
	service := NewFakeMessageBroker(mb)
	err := service.publish(ctx, "topic", nil)
	assert.Error(t, err)
	assert.Errorf(t, err, emptyTopicIDAndMsgErr)
}

func TestNewGooglePubSub_ErrorBlankProjectID(t *testing.T) {
	ctx := context.Background()
	gPubSub, err := NewGooglePubSub(ctx, "")
	assert.Errorf(t, err, emptyProjectIDErr)
	assert.Nil(t, gPubSub)
}
