package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"os"
	_ "github.com/go-sql-driver/mysql"
	"github.com/IBM/sarama"
	"github.com/vinay-winai/gomicro/internal/ledger"
)

const (
	dbDriver = "mysql"
	dbName = "ledger"
	topic = "ledger"
)

var (
	db *sql.DB
	wg sync.WaitGroup
)

type LedgerMsg struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Operation string `json:"operation"`
	Date      string `json:"date"`
}

func main() {
	var err error
	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	// Open a connection to the database
	dsn := fmt.Sprintf("%s:%s@tcp(mysql-ledger:3306)/%s", dbUser , dbPassword, dbName)
	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		log.Fatal(err)
	}
	// check if the database is alive
	defer func() {
		if err = db.Close(); err != nil {
			log.Printf("Error closing database: %s", err)
		}
	}()
	
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	sarama.Logger = log.New(os.Stdout, "[sarama]", log.LstdFlags)
	done := make(chan struct{})
	consumer, err := sarama.NewConsumer([]string{"my-cluster-kafka-bootstrap:9092"}, sarama.NewConfig())
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
	var ledgerMsg LedgerMsg
	err := json.Unmarshal(msg.Value, &ledgerMsg)
	if err != nil {
		fmt.Println("Error unmarshalling message:", err)
		return
	}
	err = ledger.Insert(db, ledgerMsg.OrderID, ledgerMsg.UserID, ledgerMsg.Amount, ledgerMsg.Operation, ledgerMsg.Date)
	if err != nil {
		fmt.Println("Error sending email:", err)
		return
	}
}