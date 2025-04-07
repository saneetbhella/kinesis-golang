#!/bin/sh
echo "Initializing LocalStack Kinesis"

awslocal kinesis create-stream \
  --stream-name data-stream \
  --shard-count 3 \

awslocal kinesis wait stream-exists \
  --stream-name data-stream

awslocal kinesis increase-stream-retention-period \
  --stream-name data-stream \
  --retention-period-hours 48

awslocal dynamodb create-table \
    --table-name KinesisCheckpoints \
    --key-schema AttributeName=ShardID,KeyType=HASH \
    --attribute-definitions AttributeName=ShardID,AttributeType=S \
    --billing-mode PAY_PER_REQUEST
