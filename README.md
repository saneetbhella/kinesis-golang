# Kinesis Golang

This repository contains applications written in Golang to produce and consume from Kinesis. Due to limitations with the
Kinesis SDK, checkpoint functionality is done manually by saving the `sequence number` in DynamoDB.

## Pre-Requisites

* Golang 1.24
* Docker

## Running

Start the infrastructure:

```bash
docker compose -f ./dev/docker-compose.yml up
```

Run the consumer:

```bash
go run ./consumer/main.go
```

Run the producer:

```bash
go run ./producer/main.go
```
