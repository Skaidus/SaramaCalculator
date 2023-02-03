package main

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"time"
)

type op int

const (
	sum op = iota
	mul
	div
	dif
)

type command struct {
	Operation op  `json:"operation"`
	Operand   int `json:"operand"`
}

type result struct {
	Num int `json:"num"`
}

// Calculator Consumes commands, updates inner number and writes results in commandsTopic
type Calculator struct {
	OperationConsumer sarama.Consumer
	ResultExporter    sarama.SyncProducer
	Number            int
	ConsumeTpc        string
	ConsumePrt        int32
	ProdTpc           string
	ProdPrt           int32
}

func (c *Calculator) Close() error {
	if err := c.ResultExporter.Close(); err != nil {
		return err
	}
	if err := c.OperationConsumer.Close(); err != nil {
		return err
	}
	return nil
}

func NewCalculator(broker []string, ConsumeTpc string, ConsumePrt int32, ProdTpc string, ProdPrt int32, cfg *sarama.Config) Calculator {
	operationConsumer, err := sarama.NewConsumer(broker, cfg)
	if err != nil {
		panic(err)
	}
	resultExporter, err := sarama.NewSyncProducer(broker, cfg)
	if err != nil {
		log.Fatal().Err(err)
	}
	return Calculator{
		OperationConsumer: operationConsumer,
		ResultExporter:    resultExporter,
		Number:            0,
		ConsumeTpc:        ConsumeTpc,
		ConsumePrt:        ConsumePrt,
		ProdTpc:           ProdTpc,
		ProdPrt:           ProdPrt,
	}
}

func (c *Calculator) Run() error {

	partitionConsumer, err := c.OperationConsumer.ConsumePartition(c.ConsumeTpc, c.ConsumePrt, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			if err := c.consumeCommand(msg); err != nil {
				return err
			}
		case <-signals:
			break ConsumerLoop
		}
	}
	return nil
}

func (c *Calculator) consumeCommand(msg *sarama.ConsumerMessage) error {
	var currCmd command
	_ = json.Unmarshal(msg.Value, &currCmd)
	switch currCmd.Operation {
	case sum:
		c.Number += currCmd.Operand
	case mul:
		c.Number *= currCmd.Operand
	case div:
		if currCmd.Operand == 0 {
			return ErrDivideByZero
		}
		c.Number /= currCmd.Operand
	case dif:
		c.Number -= currCmd.Operand
	default:
	}
	val, _ := json.Marshal(result{c.Number})
	outmsg := &sarama.ProducerMessage{Topic: c.ProdTpc, Partition: c.ProdPrt, Value: sarama.StringEncoder(val)}
	err := backoff.RetryNotify(func() (err error) {
		_, _, err = c.ResultExporter.SendMessage(outmsg)
		return
	}, backoff.NewExponentialBackOff(), func(err error, d time.Duration) {
		log.Warn().Err(err).Dur("elapsed_time", d).Msg("storage connection attempt failed, retrying...")
	})
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
		return err
	}
	return nil
}

// Producer thread that each second publishes the same command to the calculator
func produceCommands(broker []string, produceTopic string, producePartition int32) {
	producer, err := sarama.NewSyncProducer(broker, nil)
	if err != nil {
		log.Fatal().Err(err)
	}
	defer func() {
		log.Info().Msg("Shutting down produceCommands")
		if err := producer.Close(); err != nil {
			log.Fatal().Err(err)
		}
	}()
	cmd := command{
		Operation: sum,
		Operand:   1,
	}
	encmd, _ := json.Marshal(cmd)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	tick := time.Tick(1000 * time.Millisecond)
ProducerLoop:
	for {
		select {
		case <-signals:
			break ProducerLoop
		case <-tick:
			msg := &sarama.ProducerMessage{Topic: produceTopic, Partition: producePartition, Value: sarama.StringEncoder(encmd)}
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("FAILED to send message: %s\n", err)
			} else {
				log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
			}
		}
	}
}

// consumer thread that just reads results from the output topic and prints it out to std output
func consumeResults(broker []string, produceTopic string, producePartition int32) {
	consumer, err := sarama.NewConsumer(broker, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal().Err(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(produceTopic, producePartition, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		log.Info().Msg("Shutting down consumeResults")
		if err := partitionConsumer.Close(); err != nil {
			log.Fatal().Err(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	var res result
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			_ = json.Unmarshal(msg.Value, &res)
			log.Printf("Current value: %v", res.Num)
		case <-signals:
			break ConsumerLoop
		}
	}
}

func initBroker(broker []string, topics []string) {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(broker, config)
	if err != nil {
		log.Fatal().Err(err)
	}
	defer func() { _ = admin.Close() }()
	for _, tpc := range topics {
		err = admin.CreateTopic(tpc, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			log.Fatal().Err(err)
			continue
		}
		log.Info().Msgf("Created topic %v", tpc)
	}

}

func main() {
	broker := []string{"localhost:9092"}
	commandsTopic := "commands"
	resultsTopic := "results"
	initBroker(broker, []string{commandsTopic, resultsTopic})
	calc := NewCalculator(broker, commandsTopic, 0, resultsTopic, 0, nil)
	go produceCommands(broker, commandsTopic, 0)
	go consumeResults(broker, resultsTopic, 0)
	defer func() {
		log.Info().Msg("Shutting down calculator")
		if err := calc.Close(); err != nil {
			log.Fatal().Err(err)
		}
	}()
	calc.Run()
}
