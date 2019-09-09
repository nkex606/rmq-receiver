package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/nkex606/rmq-receiver/module/queue"
	"github.com/streadway/amqp"
)

// ConnWorker ...
type ConnWorker struct {
	rmqConn *amqp.Connection
	connErr chan *amqp.Error

	chanWorker []*chanWorker
}

type chanWorker struct {
	rmqCh    *amqp.Channel
	rmqQueue amqp.Queue

	name string
}

// NewConnWorker ...
func NewConnWorker() *ConnWorker {
	c := new(ConnWorker)
	err := c.initRMQ()
	if err != nil {
		panic(err)
	}
	return c
}

func (c *ConnWorker) initRMQ() error {
	var err error

	err = c.dialRMQ()
	if err != nil {
		log.Println("Dial RMQ fail.")
		return err
	}

	c.chanWorker = make([]*chanWorker, 4)

	for i := 0; i < len(c.chanWorker); i++ {
		cw := &chanWorker{
			name: fmt.Sprintf("consumer-%d", i),
		}
		cw.setupChannelAndQueue(c.rmqConn)
		c.chanWorker[i] = cw
	}
	return nil
}

func (c *ConnWorker) dialRMQ() error {
	var err error

	c.rmqConn, err = queue.NewConnection("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("Create connection fail.")
		return err
	}
	log.Println("Rabbitmq connected.")
	c.connErr = make(chan *amqp.Error)
	c.rmqConn.NotifyClose(c.connErr)

	go c.reconnect()
	return nil
}

func (cw *chanWorker) setupChannelAndQueue(conn *amqp.Connection) error {
	var err error

	cw.rmqCh, err = queue.NewChannel(conn, 10)
	if err != nil {
		log.Printf("Create channel %s fail.", cw.name)
		return err
	}

	cw.rmqQueue, err = queue.NewQueue(cw.rmqCh, "task_queue")
	if err != nil {
		log.Println("create queue fail")
		return err
	}

	return nil
}

func (c *ConnWorker) reconnect() {
	for {
		e := <-c.connErr
		if e != nil {
			for {
				err := c.dialRMQ()
				if err == nil {
					log.Println("RMQ reconnect.")

					for _, cw := range c.chanWorker {
						err = cw.setupChannelAndQueue(c.rmqConn)
						if err == nil {
							log.Printf("%s reconnect.", cw.name)
						}
					}
					return
				}
				log.Println("Trying connect to RMQ...")
				<-time.After(time.Second)
			}

		}
	}
}

func (cw *chanWorker) consume() error {
	msgs, err := cw.rmqCh.Consume(
		cw.rmqQueue.Name, // queue
		cw.name,          // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		log.Println("Consume message fail.")
		return err
	}

	go func() {
		for d := range msgs {
			log.Printf("%s received a message: %s", cw.name, d.Body)
			d.Ack(false)
		}
	}()
	return nil
}

// StartConsume ...
func (c *ConnWorker) StartConsume() error {
	for _, cw := range c.chanWorker {
		go func(cw *chanWorker) error {
			var err error
			err = cw.consume()
			if err != nil {
				log.Printf("%s fail.", cw.name)
				return err
			}
			return nil
		}(cw)
	}
	return nil
}
