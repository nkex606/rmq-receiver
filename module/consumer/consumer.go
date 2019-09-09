package consumer

import (
	"fmt"
	"log"
	"time"

	"github.com/nkex606/rmq-receiver/module/queue"

	"github.com/streadway/amqp"
)

// Consumer ...
type Consumer struct {
	rmqConn  *amqp.Connection
	rmqCh    *amqp.Channel
	rmqQueue amqp.Queue

	chErr chan *amqp.Error

	workers     int
	workersName map[string]string
}

// NewConsumer ...
func NewConsumer() (c *Consumer) {
	c = &Consumer{
		workers: 4,
	}

	e := c.initRMQ()
	if e != nil {
		panic(e)
	}
	return
}

func (c *Consumer) initRMQ() (err error) {
	c.dialRMQ()

	c.rmqCh, err = queue.NewChannel(
		c.rmqConn,
		10, // prefetch count
	)
	if err != nil {
		log.Println("create channel fail")
		return err
	}

	c.rmqQueue, err = queue.NewQueue(c.rmqCh, "task_queue")
	if err != nil {
		log.Println("create queue fail")
		return err
	}
	return
}

func (c *Consumer) dialRMQ() error {
	var err error
	c.rmqConn, err = queue.NewConnection("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("Create connection fail.")
		return err
	}
	log.Println("Rabbitmq connected.")
	c.chErr = make(chan *amqp.Error)
	c.rmqConn.NotifyClose(c.chErr)

	go c.reconnect()
	return nil
}

func (c *Consumer) reconnect() {
	for {
		e := <-c.chErr
		if e != nil {
			for {
				err := c.dialRMQ()
				if err == nil {
					log.Println("RMQ reconnect.")
					return
				}
				log.Println("Trying connect to RMQ...")
				<-time.After(time.Second)
			}
		}
	}
}

// StartConsume ...
func (c *Consumer) StartConsume() (err error) {
	c.workersName = make(map[string]string)
	for i := 0; i < c.workers; i++ {
		name := fmt.Sprintf("consumer-%d", i)
		c.workersName[name] = name
		e := c.newMsgConsumer(name)
		if e != nil {
			log.Printf("%s fail", name)
			return
		}
	}
	return
}

func (c *Consumer) newMsgConsumer(name string) (err error) {
	msgs, err := c.rmqCh.Consume(
		c.rmqQueue.Name, // queue
		name,            // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		log.Println("Consume message fail.")
		return
	}

	go func() {
		for d := range msgs {
			log.Printf("%s received a message: %s", name, d.Body)
			d.Ack(false)
		}
	}()
	return
}

// StopConsume ...
func (c *Consumer) StopConsume() {
	for _, w := range c.workersName {
		err := c.rmqCh.Cancel(c.workersName[w], false)
		if err != nil {
			log.Printf("Cancel %s fail", c.workersName[w])
			return
		}
	}
}
