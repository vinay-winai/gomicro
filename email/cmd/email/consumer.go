package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"os"
	"github.com/IBM/sarama"
	"github.com/vinay-winai/gomicro/internal/email"
)

const topic = "email"

var wg sync.WaitGroup

type EmailMsg struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}


func main() {
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
	err = email.Send(emailMsg.UserID, emailMsg.OrderID)
	if err != nil {
		fmt.Println("Error sending email:", err)
		return
	}
}