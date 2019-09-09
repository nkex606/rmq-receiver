package main

import (
	"log"

	"github.com/nkex606/rmq-receiver/module/worker"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	w := worker.NewConnWorker()
	w.StartConsume()

	forever := make(chan struct{})
	<-forever
}
