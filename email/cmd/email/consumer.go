package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/vinay-winai/gomicro/internal/email"
)

const topic = "email"

var wg sync.WaitGroup

type EmailMsg struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}

var cache *redis.Client
var ctx = context.Background()
var redisClientName = strconv.FormatInt(time.Now().UnixNano(), 10)

func initRedis(ctx context.Context) {
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST"),
		Password: os.Getenv("REDIS_PASSWORD"),
		ClientName: redisClientName,
	})
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Error connecting to Redis:", err)
	}
	cache = client
}

func main() {
	initRedis(ctx)
	sarama.Logger = log.New(os.Stdout, "[sarama]", log.LstdFlags)
	done := make(chan struct{})
	config := sarama.NewConfig()
	
	consumer, err := sarama.NewConsumer([]string{"my-cluster-kafka-bootstrap:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		close(done)
		if err := consumer.Close(); err != nil {
			log.Println(err)
		}
	}()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := pc.Close(); err != nil {
				log.Println(err)
			}
		}()
		wg.Add(1)
		go awaitMessages(pc, partition, done)
	}
	wg.Wait()
}

func awaitMessages(pc sarama.PartitionConsumer, partition int32, done chan struct{}) {
	defer wg.Done()

	for {
		select {
		case msg := <-pc.Messages():
			fmt.Printf("Partition %d - Received message: %s\n", partition, string(msg.Value))
			handleMessage(msg)
		case <-done:
			fmt.Println("Received done signal. Exiting...")
			return
		}
	}
}

func handleMessage(msg *sarama.ConsumerMessage) {
	var emailMsg EmailMsg
	err := json.Unmarshal(msg.Value, &emailMsg)
	if err != nil {
		fmt.Println("Error unmarshalling message:", err)
		return
	}
	cacheKey := strconv.Itoa(time.Now().Second())
	// clientName := cache.ClientGetName(ctx).FullName() and .Name() always return "client".
	// maybe issue with the package.
	success,_ := cache.HSetNX(ctx, cacheKey, emailMsg.OrderID, redisClientName).Result()
	cache.ExpireNX(ctx, cacheKey, time.Second)
	// once
	if !success {
		log.Println("OrderID already exists in redis-cache")
		return
	}
	log.Println("Added orderID to redis-cache")
	email.Send(emailMsg.UserID, emailMsg.OrderID)
	log.Printf("Sent email with Order-id:%s", emailMsg.OrderID)
}