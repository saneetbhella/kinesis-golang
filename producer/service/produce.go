package service

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"github.com/saneetbhella/logger"
)

type KinesisProducer interface {
	Produce(string, []byte) (*kinesis.PutRecordOutput, error)
}

func Start(kc KinesisProducer) {
	for i := 0; i < 1; i++ {
		output, err := kc.Produce(*aws.String(uuid.New().String()), []byte("test"))

		if err != nil {
			logger.Errorf("Error putting record %v", err)
		} else {
			fmt.Printf("Successfully put record with sequence number %v on shard %v\n", *output.SequenceNumber, *output.ShardId)
		}
	}
}
