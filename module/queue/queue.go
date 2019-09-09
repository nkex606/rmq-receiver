package queue

import (
	"github.com/streadway/amqp"
)

// NewConnection create a connection to url
func NewConnection(url string) (conn *amqp.Connection, err error) {
	conn, err = amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return
}

// NewChannel create a channel to connection
func NewChannel(conn *amqp.Connection, prefetch int) (ch *amqp.Channel, err error) {
	ch, err = conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = ch.Qos(
		prefetch, // prefetch count
		0,        // prefetch size
		false,    // global
	); err != nil {
		return nil, err
	}
	return
}

// NewQueue declare a queue with the channel and given name
func NewQueue(ch *amqp.Channel, name string) (queue amqp.Queue, err error) {
	queue, err = ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return queue, err
	}
	return
}
