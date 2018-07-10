package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
	"github.com/urfave/cli"
)

var totalTime int64 = 0
var totalCount int64 = 0

type MqMessage struct {
	TimeNow        time.Time
	SequenceNumber int
	Payload        string
}

func main() {
	app := cli.NewApp()
	app.Name = "tester"
	app.Usage = "Make the rabbit cry"
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "server, s", Value: "rabbit-mq-test.cs1cloud.internal", Usage: "Hostname for RabbitMQ server"},
		cli.StringFlag{Name: "port, P", Value: "5672", Usage: "Port for RabbitMQ server"},
		cli.StringFlag{Name: "user, u", Value: "guest", Usage: "user for RabbitMQ server"},
		cli.StringFlag{Name: "password, pass", Value: "guest", Usage: "user password for RabbitMQ server"},
		cli.StringFlag{Name: "vhost", Value: "", Usage: "vhost for RabbitMQ server"},
		cli.StringSliceFlag{Name: "consumer-arg", Value: &cli.StringSlice{}, Usage: "Optional `key=value` pair to be sent to consumer arguments, repeat for multiple."},
		cli.StringSliceFlag{Name: "queue-arg", Value: &cli.StringSlice{}, Usage: "Optional `key=value` pair to be sent to queue declaration arguments, repeat for multiple."},
		cli.IntFlag{Name: "producer, p", Value: 0, Usage: "Number of messages to produce, -1 to produce forever"},
		cli.IntFlag{Name: "wait, w", Value: 0, Usage: "Number of nanoseconds to wait between publish events"},
		cli.IntFlag{Name: "consumer, c", Value: -1, Usage: "Number of messages to consume. 0 consumes forever"},
		cli.IntFlag{Name: "bytes, b", Value: 0, Usage: "number of extra bytes to add to the RabbitMQ message payload. About 50K max"},
		cli.IntFlag{Name: "concurrency, n", Value: 50, Usage: "number of reader/writer Goroutines"},
		cli.BoolFlag{Name: "quiet, q", Usage: "Print only errors to stdout"},
		cli.BoolFlag{Name: "wait-for-ack, a", Usage: "Wait for an ack or nack after enqueueing a message"},
	}
	app.Action = func(c *cli.Context) error {
		runApp(c)
		return nil
	}
	app.Run(os.Args)
}

func runApp(c *cli.Context) {
	println("Running!")

	queueArgs, err := parseArgs(c.StringSlice("queue-arg"))
	if err != nil {
		log.Fatalf("Error parsing queue-arg: %v", err)
	}

	consumerArgs, err := parseArgs(c.StringSlice("consumer-arg"))
	if err != nil {
		log.Fatalf("Error parsing consumer-arg: %v", err)
	}

	porto := "amqp://"
	uri := porto + c.String("user") + ":" + c.String("password") + "@" + c.String("server") +
		":" + c.String("port")

	if c.String("vhost") != "" {
		uri += "/" + c.String("vhost")
	}

	if c.Int("consumer") > -1 {
		makeConsumers(uri, c.Int("concurrency"), c.Int("consumer"), queueArgs, consumerArgs)
	}

	if c.Int("producer") != 0 {
		config := ProducerConfig{uri, c.Int("bytes"), c.Bool("quiet"), c.Bool("wait-for-ack")}
		makeProducers(c.Int("producer"), c.Int("wait"), c.Int("concurrency"), config, queueArgs)
	}
}

func MakeQueue(c *amqp.Channel, args amqp.Table) amqp.Queue {
	q, err := c.QueueDeclare("stress-test-exchange", true, false, false, false, args)
	if err != nil {
		log.Fatal(err)
	}
	return q
}

func makeProducers(n int, wait int, concurrency int, config ProducerConfig, queueArgs amqp.Table) {

	taskChan := make(chan int)
	for i := 0; i < concurrency; i++ {
		go Produce(config, taskChan, queueArgs)
	}

	start := time.Now()

	for i := 0; i < n; i++ {
		taskChan <- i
		time.Sleep(time.Duration(int64(wait)))
	}

	time.Sleep(time.Duration(10000))

	close(taskChan)

	log.Printf("Finished: %s", time.Since(start))
}

func makeConsumers(uri string, concurrency int, toConsume int, queueArgs amqp.Table, consumerArgs amqp.Table) {

	doneChan := make(chan bool)

	for i := 0; i < concurrency; i++ {
		go Consume(uri, doneChan, queueArgs, consumerArgs)
	}

	start := time.Now()

	if toConsume > 0 {
		for i := 0; i < toConsume; i++ {
			<-doneChan
			if i == 1 {
				start = time.Now()
			}
			log.Println("Consumed: ", i)
		}
	} else {

		for {
			<-doneChan
		}
	}

	log.Printf("Done consuming! %s", time.Since(start))
}

func parseArgs(argSet []string) (amqp.Table, error) {
	args := amqp.Table{}
	for _, argKv := range argSet {
		argKVSplit := strings.SplitN(argKv, "=", 2)
		if len(argKVSplit) != 2 {
			return nil, fmt.Errorf("invalid key value pair '%v'", argKv)
		}

		switch argKVSplit[1] {
		case "true":
			args[argKVSplit[0]] = true
		case "false":
			args[argKVSplit[0]] = false
		default:
			if asInt, err := strconv.Atoi(argKVSplit[0]); err == nil {
				args[argKVSplit[0]] = asInt
			} else {
				args[argKVSplit[0]] = argKVSplit[1]
			}
		}
	}
	return args, nil
}
