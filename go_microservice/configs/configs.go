package configs

import (
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
)

type AppConfig struct {
	RabbitMQDSN   string
	MongoDSN      string
	ServiceName   string
	NextService   string
	ServiceConfig map[string]string
}

func GetServiceConfig(mongoDsn string, serviceName string) map[string]string {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(mongoDsn).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	var result bson.M
	findErr := client.Database("services_configs").Collection("services_configs").FindOne(context.TODO(), bson.D{{"service", serviceName}}).Decode(&result)
	if findErr != nil {
		panic(findErr)
	}

	var jsonConfig map[string]string

	res, _ := bson.MarshalExtJSON(result, false, false)
	err = json.Unmarshal(res, &jsonConfig)
	return jsonConfig
}

func GetAppConfig() *AppConfig {
	config := &AppConfig{}
	rabbitmqDsn, _ := os.LookupEnv("RABBITMQ_DSN")
	mongoDsn, _ := os.LookupEnv("MONGO_DSN")
	serviceName, _ := os.LookupEnv("SERVICE_NAME")
	config.RabbitMQDSN = rabbitmqDsn
	config.MongoDSN = mongoDsn
	config.ServiceName = serviceName
	config.ServiceConfig = GetServiceConfig(mongoDsn, serviceName)
	config.NextService = config.ServiceConfig["next_service"]
	return config
}
