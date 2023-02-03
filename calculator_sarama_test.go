package main

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDivideByZeroMockBroker(t *testing.T) {
	broker := sarama.NewMockBroker(t, 0)
	defer broker.Close()
	cmd := command{
		Operation: div,
		Operand:   0,
	}
	encmd, _ := json.Marshal(cmd)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(commandsTopic, 0, broker.BrokerID()),
		//SetLeader(resultsTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(commandsTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(commandsTopic, 0, sarama.OffsetNewest, 1),

		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage(commandsTopic, 0, 1, sarama.StringEncoder(encmd)),
	})

	calc := NewCalculator([]string{broker.Addr()}, commandsTopic, 0, resultsTopic, 0, nil)

	res := calc.Run()
	require.Equal(t, res, ErrDivideByZero)
}

func TestFirstSendFailsMockBroker(t *testing.T) {
	broker := sarama.NewMockBroker(t, 0)
	defer broker.Close()
	cmd := command{
		Operation: sum,
		Operand:   0,
	}
	config := mocks.NewTestConfig()
	config.Producer.Return.Successes = true

	encmd, _ := json.Marshal(cmd)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(commandsTopic, 0, broker.BrokerID()).
			SetLeader(resultsTopic, 0, broker.BrokerID()),
		"ProduceRequest": sarama.NewMockSequence(
			sarama.NewMockProduceResponse(t).SetError(resultsTopic, 0, sarama.ErrBrokerNotAvailable),
			sarama.NewMockProduceResponse(t).SetError(resultsTopic, 0, sarama.ErrNoError),
		),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(commandsTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(commandsTopic, 0, sarama.OffsetNewest, 1).
			SetOffset(resultsTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(resultsTopic, 0, sarama.OffsetNewest, 1),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage(commandsTopic, 0, 1, sarama.StringEncoder(encmd)),
	})

	calc := NewCalculator([]string{broker.Addr()}, commandsTopic, 0, resultsTopic, 0, config)
	pc2, err := calc.OperationConsumer.ConsumePartition(commandsTopic, 0, sarama.OffsetNewest)
	require.NoError(t, err)
	err = calc.consumeCommand(<-pc2.Messages())
	require.NoError(t, err)
}
