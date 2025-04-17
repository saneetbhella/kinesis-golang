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

func (c *Client) Describe() error {
	_, err := c.kinesisClient.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{
		StreamName: &c.streamName,
	})

	return err
}

func (c *Client) Produce(partitionKey string, data []byte) (*kinesis.PutRecordOutput, error) {
	output, err := c.kinesisClient.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   &c.streamName,
		Data:         data,
		PartitionKey: aws.String(partitionKey),
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}
