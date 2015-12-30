package messagebroker

type Publisher interface {
	Publish(routing string, body []byte) error
}

type Consumer interface {
	Consume(queue string, onMessage func(body []byte)) error
}

type MessageBroker interface {
	Publisher
	Consumer
}
