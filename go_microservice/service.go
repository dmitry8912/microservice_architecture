package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"msa-go-srv/configs"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	cfg := configs.GetAppConfig()
	log.Printf("Startup config %v", cfg)
	conn, err := amqp.Dial(cfg.RabbitMQDSN)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		fmt.Sprintf("%v_requests", cfg.ServiceName), // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for d := range msgs {
			var request map[string]any
			err := json.Unmarshal(d.Body, &request)

			failOnError(err, "Failed to convert body to integer")

			log.Printf("Received request %v", request)

			result := map[string]any{}
			result["service"] = cfg.ServiceName
			subResult := map[string]any{}

			subResult["status"] = "success"
			subResult["data"] = uuid.New().String()
			subResult["request"] = request
			subResult["start_at"] = time.Now().String()
			subResult["end_at"] = time.Now().String()

			result["result"] = subResult

			responseBytes, _ := json.Marshal(result)

			err = ch.PublishWithContext(ctx,
				"", // exchange
				fmt.Sprintf("%v_requests", cfg.NextService), // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          responseBytes,
				})
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf("Started consumer")
	<-forever
}
