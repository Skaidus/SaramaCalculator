package main

import (
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestDivideByZeroMockProdCon(t *testing.T) {
	commandConsumer := mocks.NewConsumer(t, nil)
	pc := commandConsumer.ExpectConsumePartition(commandsTopic, 0, sarama.OffsetNewest)
	pc.ExpectMessagesDrainedOnClose()
	cmd := command{
		Operation: div,
		Operand:   0,
	}
	encmd, _ := json.Marshal(cmd)
	pc.YieldMessage(&sarama.ConsumerMessage{Topic: commandsTopic, Partition: 0, Value: encmd})

	resultProducer := mocks.NewSyncProducer(t, nil)

	calc := Calculator{
		OperationConsumer: commandConsumer,
		ResultExporter:    resultProducer,
		Number:            0,
		ConsumeTpc:        commandsTopic,
		ConsumePrt:        0,
		ProdTpc:           resultsTopic,
		ProdPrt:           0,
	}
	defer safeClose(t, &calc)
	res := calc.Run()
	if res != ErrDivideByZero {
		t.Errorf("got nil; want ErrDivideByZero")
	}
}

func TestFirstSendFailsMockProdConNoChecks(t *testing.T) {
	commandConsumer := mocks.NewConsumer(t, nil)
	pc := commandConsumer.ExpectConsumePartition(commandsTopic, 0, sarama.OffsetNewest)
	pc.ExpectMessagesDrainedOnClose()

	cmd := command{
		Operation: sum,
		Operand:   0,
	}
	encmd, _ := json.Marshal(cmd)
	pc.YieldMessage(&sarama.ConsumerMessage{Topic: commandsTopic, Partition: 0, Value: encmd})

	resultProducer := mocks.NewSyncProducer(t, nil)
	resultProducer.ExpectSendMessageAndFail(sarama.ErrKafkaStorageError)
	resultProducer.ExpectSendMessageAndSucceed()

	calc := Calculator{
		OperationConsumer: commandConsumer,
		ResultExporter:    resultProducer,
		Number:            0,
		ConsumeTpc:        commandsTopic,
		ConsumePrt:        0,
		ProdTpc:           resultsTopic,
		ProdPrt:           0,
	}

	defer safeClose(t, &calc)
	pc2, err := calc.OperationConsumer.ConsumePartition(commandsTopic, 0, sarama.OffsetNewest)
	require.NoError(t, err)
	err = calc.consumeCommand(<-pc2.Messages())
	require.NoError(t, err)
}

var checkUnmarshal = mocks.ValueChecker(func(val []byte) error {
	var currCmd command
	return json.Unmarshal(val, &currCmd)
})

var checkResultMessage = mocks.MessageChecker(func(msg *sarama.ProducerMessage) error {
	if msg.Topic != resultsTopic {
		return errors.New("invalid topic name")
	}
	val, _ := msg.Value.Encode()
	mocks.NewTestConfig()
	return checkUnmarshal(val)
})

func TestFirstSendFailsMockProdConWithChecks(t *testing.T) {
	commandConsumer := mocks.NewConsumer(t, nil)
	pc := commandConsumer.ExpectConsumePartition(commandsTopic, 0, sarama.OffsetNewest)
	pc.ExpectMessagesDrainedOnClose()

	cmd := command{
		Operation: sum,
		Operand:   0,
	}
	encmd, _ := json.Marshal(cmd)
	pc.YieldMessage(&sarama.ConsumerMessage{Topic: commandsTopic, Partition: 0, Value: encmd})

	resultProducer := mocks.NewSyncProducer(t, nil)
	resultProducer.ExpectSendMessageWithCheckerFunctionAndFail(checkUnmarshal, sarama.ErrKafkaStorageError)
	resultProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(checkResultMessage)

	calc := Calculator{
		OperationConsumer: commandConsumer,
		ResultExporter:    resultProducer,
		Number:            0,
		ConsumeTpc:        commandsTopic,
		ConsumePrt:        0,
		ProdTpc:           resultsTopic,
		ProdPrt:           0,
	}

	defer safeClose(t, &calc)
	pc2, err := calc.OperationConsumer.ConsumePartition(commandsTopic, 0, sarama.OffsetNewest)
	require.NoError(t, err)
	err = calc.consumeCommand(<-pc2.Messages())
	require.NoError(t, err)
}

func safeClose(t *testing.T, o io.Closer) {
	if err := o.Close(); err != nil {
		t.Error(err)
	}
}
