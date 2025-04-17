package service

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/saneetbhella/logger"
	"sync"
	"time"
)

type KinesisConsumer interface {
	GetShardIterator(*kinesis.GetShardIteratorInput) (*string, error)
	GetRecords(string) (*kinesis.GetRecordsOutput, error)
}

type DynamoDbClient interface {
	GetCheckpoint(string) (*string, error)
	CommitCheckpoint(string, string) error
}

func Start(kc KinesisConsumer, shardsOutput *kinesis.DescribeStreamOutput, pollConfigIntervalSeconds int, ddc DynamoDbClient) {
	var wg sync.WaitGroup

	// todo: sigterm end wg

	for _, shard := range shardsOutput.StreamDescription.Shards {
		wg.Add(1)
		logger.Infof("Launching consumer for shard %v", *shard.ShardId)
		go consume(kc, *shard.ShardId, *shardsOutput.StreamDescription.StreamName, pollConfigIntervalSeconds, ddc)
	}

	wg.Wait()
}

func consume(kc KinesisConsumer, shardId string, streamName string, pollConfigIntervalSeconds int, ddc DynamoDbClient) {
	checkpoint, err := ddc.GetCheckpoint(shardId)

	if err != nil {
		logger.Fatalf("Error getting checkpoint %v", err)
	}

	iteratorType := types.ShardIteratorTypeTrimHorizon
	input := &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           &shardId,
		ShardIteratorType: iteratorType,
	}

	if checkpoint != nil {
		logger.Infof("Reading from checkpoint %v for shard %v", *checkpoint, shardId)
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = aws.String(*checkpoint)
	}

	iterator, err := kc.GetShardIterator(input)
	if err != nil {
		logger.Fatalf("Failed to get shard iterator: %v", err)
	}

	for {
		pollDelayInSeconds(pollConfigIntervalSeconds)

		records, err := kc.GetRecords(*iterator)

		if err != nil {
			logger.Errorf("Failed to get records: %v", err)
			continue
		}

		if len(records.Records) > 0 {
			for _, record := range records.Records {
				// todo: schema registry
				logger.Infof("Record: %v", string(record.Data))

				// todo: checkpointing
				logger.Infof("Saving checkpoint at %v for shard %v", *record.SequenceNumber, shardId)
				if err := ddc.CommitCheckpoint(shardId, *record.SequenceNumber); err != nil {
					logger.Errorf("Failed to commit checkpoint: %v", err)
				}
			}
		}

		iterator = records.NextShardIterator
	}
}

func pollDelayInSeconds(amount int) {
	time.Sleep(time.Duration(amount) * time.Second)
}
