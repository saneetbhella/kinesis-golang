package client

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	appConfig "kinesis-streams/config"
)

type Client struct {
	kinesisClient *kinesis.Client
	streamName    string
}

func New(c appConfig.KinesisConfig) *Client {
	staticCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
		c.AccessKeyId,
		c.SecretAccessKey,
		"",
	))

	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(staticCreds),
	)

	kinesisClient := kinesis.NewFromConfig(cfg, func(o *kinesis.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
	})

	return &Client{
		kinesisClient: kinesisClient,
		streamName:    c.StreamName,
	}
}

func (c *Client) Describe() (*kinesis.DescribeStreamOutput, error) {
	shardOutputs, err := c.kinesisClient.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{
		StreamName: &c.streamName,
	})

	return shardOutputs, err
}

func (c *Client) GetShardIterator(shardIterator *kinesis.GetShardIteratorInput) (*string, error) {
	i, err := c.kinesisClient.GetShardIterator(context.Background(), shardIterator)

	return i.ShardIterator, err
}

func (c *Client) GetRecords(shardIterator string) (*kinesis.GetRecordsOutput, error) {
	r, err := c.kinesisClient.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: &shardIterator,
	})

	return r, err
}
