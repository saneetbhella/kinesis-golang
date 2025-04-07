package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/saneetbhella/logger"
	"kinesis-streams/config"
)

type Client struct {
	TableName string
	Client    *dynamodb.DynamoDB
}

type Checkpoint struct {
	ShardID        string
	SequenceNumber string
}

func New(c config.DynamoDbConfig) *Client {
	sess, _ := session.NewSession(
		&aws.Config{
			Region:      aws.String(c.Region),
			Endpoint:    aws.String(c.Endpoint),
			Credentials: credentials.NewStaticCredentials(c.AccessKeyId, c.SecretAccessKey, ""),
		},
	)

	return &Client{
		Client:    dynamodb.New(sess),
		TableName: c.CheckpointsTableName,
	}
}

func (c *Client) GetCheckpoint(shardId string) (*string, error) {
	r, err := c.Client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(c.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: aws.String(shardId),
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if r.Item == nil {
		return nil, nil
	}

	checkpoint := Checkpoint{}
	err = dynamodbattribute.UnmarshalMap(r.Item, &checkpoint)

	if err != nil {
		return nil, err
	}

	return &checkpoint.SequenceNumber, err
}

func (c *Client) CommitCheckpoint(shardID string, sequenceNumber string) error {
	av, err := dynamodbattribute.MarshalMap(Checkpoint{
		ShardID:        shardID,
		SequenceNumber: sequenceNumber,
	})

	if err != nil {
		logger.Errorf("Error marshalling checkpoint %v", err)
		return err
	}

	_, err = c.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(c.TableName),
		Item:      av,
	})

	if err != nil {
		logger.Errorf("Error putting checkpoint %v", err)
		return err
	}

	return nil
}
