package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/saneetbhella/logger"
	appConfig "kinesis-streams/config"
)

type Client struct {
	TableName string
	Client    *dynamodb.Client
}

type Checkpoint struct {
	ShardID        string
	SequenceNumber string
}

func New(c appConfig.DynamoDbConfig) *Client {
	staticCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
		c.AccessKeyId,
		c.SecretAccessKey,
		"",
	))

	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(staticCreds),
	)

	dynamoDbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
	})

	return &Client{
		Client:    dynamoDbClient,
		TableName: c.CheckpointsTableName,
	}
}

func (c *Client) GetCheckpoint(shardId string) (*string, error) {
	r, err := c.Client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(c.TableName),
		Key: map[string]types.AttributeValue{
			"ShardID": &types.AttributeValueMemberS{Value: shardId},
		},
	})

	if err != nil {
		return nil, err
	}

	if r.Item == nil {
		return nil, nil
	}

	checkpoint := Checkpoint{}
	err = attributevalue.UnmarshalMap(r.Item, &checkpoint)

	if err != nil {
		return nil, err
	}

	return &checkpoint.SequenceNumber, err
}

func (c *Client) CommitCheckpoint(shardID string, sequenceNumber string) error {
	av, err := attributevalue.MarshalMap(Checkpoint{
		ShardID:        shardID,
		SequenceNumber: sequenceNumber,
	})

	if err != nil {
		logger.Errorf("Error marshalling checkpoint %v", err)
		return err
	}

	_, err = c.Client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(c.TableName),
		Item:      av,
	})

	if err != nil {
		logger.Errorf("Error putting checkpoint %v", err)
		return err
	}

	return nil
}
