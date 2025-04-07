package client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"kinesis-streams/config"
)

type Client struct {
	kinesisClient *kinesis.Kinesis
	streamName    string
}

func New(c config.KinesisConfig) *Client {
	s, _ := session.NewSession(
		&aws.Config{
			Region:      aws.String(c.Region),
			Endpoint:    aws.String(c.Endpoint),
			Credentials: credentials.NewStaticCredentials(c.AccessKeyId, c.SecretAccessKey, ""),
		},
	)

	kc := kinesis.New(s)

	return &Client{
		kinesisClient: kc,
		streamName:    c.StreamName,
	}
}

func (c *Client) Describe() (*kinesis.DescribeStreamOutput, error) {
	shardOutputs, err := c.kinesisClient.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &c.streamName,
	})

	return shardOutputs, err
}

func (c *Client) GetShardIterator(shardIterator *kinesis.GetShardIteratorInput) (*string, error) {
	i, err := c.kinesisClient.GetShardIterator(shardIterator)

	return i.ShardIterator, err
}

func (c *Client) GetRecords(shardIterator string) (*kinesis.GetRecordsOutput, error) {
	r, err := c.kinesisClient.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: &shardIterator,
	})

	return r, err
}
