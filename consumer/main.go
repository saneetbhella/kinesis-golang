package main

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/saneetbhella/logger"
	"kinesis-streams/config"
	"kinesis-streams/consumer/client"
	"kinesis-streams/consumer/service"
	"kinesis-streams/dynamodb"
)

func main() {
	if err := godotenv.Load(); err != nil {
		logger.Fatalf("Error loading .env file %v", err)
	}

	var kinesisConfig config.KinesisConfig
	if err := env.Parse(&kinesisConfig); err != nil {
		logger.Fatalf("Error parsing .env file %v", err)
	}

	var dynamoDbConfig config.DynamoDbConfig
	if err := env.Parse(&dynamoDbConfig); err != nil {
		logger.Fatalf("Error parsing .env file %v", err)
	}

	ddc := dynamodb.New(dynamoDbConfig)

	kc := client.New(kinesisConfig)
	output, err := kc.Describe()

	if err != nil {
		logger.Fatalf("Error describing stream %v", err)
	}

	service.Start(kc, output, kinesisConfig.PollConfigIntervalSeconds, ddc)
}
