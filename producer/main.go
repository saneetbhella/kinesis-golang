package main

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/saneetbhella/logger"
	"kinesis-streams/config"
	"kinesis-streams/producer/client"
	"kinesis-streams/producer/service"
)

func main() {
	if err := godotenv.Load(); err != nil {
		logger.Fatalf("Error loading .env file %v", err)
	}

	var c config.KinesisConfig
	if err := env.Parse(&c); err != nil {
		logger.Fatalf("Error parsing .env file %v", err)
	}

	kc := client.New(c)
	if err := kc.Describe(); err != nil {
		logger.Fatalf("Error describing stream %v", err)
	}

	service.Start(kc)
}
