package email

import (
	"fmt"
	"net/smtp"
	"os"
)

func Send(target string,orderID string) error {
	senderEmail := os.Getenv("SENDER_EMAIL")
	password := os.Getenv("SENDER_PASSWORD")

	
	recipientEmail := target
	message := []byte(fmt.Sprintf("Subject: Payment Processed!\n Process ID: %s\n", orderID))

	smtpServer := "smtp.gmail.com"
	smtpPort := 587

	creds := smtp.PlainAuth("", senderEmail, password, smtpServer)
	smtpAddress := fmt.Sprintf("%s:%d", smtpServer, smtpPort)
	err := smtp.SendMail(smtpAddress, creds, senderEmail, []string{recipientEmail}, message)
	if err != nil {
		return err
	}
	return nil
}