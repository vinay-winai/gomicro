package producer

import (
	"encoding/json"
	"log"
	"sync"
	"time"
	"os"
	"github.com/IBM/sarama"
)

const (
	emailTopic  = "email"
	ledgerTopic = "ledger"
)

type EmailMsg struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}

type LedgerMsg struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Operation string `json:"operation"`
	Date      string `json:"date"`
}

func SendCaptureMessage(pid string, userID string, amount int64) {
	sarama.Logger = log.New(os.Stdout, "[sarama]", log.LstdFlags)
	config := sarama.NewConfig()
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.ID = pid
	
	// Create a sync producer
	producer, err := sarama.NewSyncProducer([]string{"my-cluster-kafka-bootstrap:9092"}, config)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Println(err)
		}
	}()
	// Begin a new transaction
	err = producer.BeginTxn()
	if err != nil {
		log.Println(err)
		producer.AbortTxn()
		return
	} 

	emailMsg := EmailMsg{
		OrderID: pid,
		UserID:  userID,
	}
	
	ledgerMsg := LedgerMsg{
		OrderID:   pid,
		UserID:    userID,
		Amount:    amount,
		Operation: "DEBIT",
		Date:      time.Now().Format("2006-01-02"),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go sendMsg(producer, emailMsg, emailTopic, &wg)
	go sendMsg(producer, ledgerMsg, ledgerTopic, &wg)
	wg.Wait()

	producer.CommitTxn()


}

func sendMsg[T EmailMsg | LedgerMsg](producer sarama.SyncProducer, msg T, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		log.Println(err)
		producer.AbortTxn()
		return
	}
	
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(stringMsg),
	}
	// send message to the Kafka topic
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Println(err)
		producer.AbortTxn()
		return
	}
	log.Printf("Message sent to partition %d at offset %d", partition, offset)

}