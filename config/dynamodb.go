package config

type DynamoDbConfig struct {
	Region               string `env:"REGION,required"`
	Endpoint             string `env:"ENDPOINT,required"`
	AccessKeyId          string `env:"AWS_ACCESS_KEY_ID,required"`
	SecretAccessKey      string `env:"AWS_SECRET_ACCESS_KEY,required"`
	CheckpointsTableName string `env:"CHECKPOINTS_TABLE_NAME,required"`
}
