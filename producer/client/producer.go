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

func (c *Client) Describe() error {
	_, err := c.kinesisClient.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &c.streamName,
	})

	return err
}

func (c *Client) Produce(partitionKey string, data []byte) (*kinesis.PutRecordOutput, error) {
	output, err := c.kinesisClient.PutRecord(&kinesis.PutRecordInput{
		StreamName:   &c.streamName,
		Data:         data,
		PartitionKey: aws.String(partitionKey),
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}
