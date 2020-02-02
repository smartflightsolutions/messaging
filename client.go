package messaging

type PubSubClient interface {
	Publish(opts PublishOptions)
}
