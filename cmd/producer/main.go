package main

import (
	"context"
	"log"
	"time"

	"go-kafka/internal/usecase"
	"go-kafka/pkg/taskqueue"
)

// ProducerExample shows how to publish a task to Kafka
func main() {
	ctx := context.Background()

	// Initialize Kafka publisher
	config := taskqueue.DefaultKafkaConfig(
		[]string{"localhost:8667"},
		"notification-group",
	)
	config.DefaultTopic = "notification-topic"

	queue := taskqueue.NewKafkaTaskQueue[usecase.NotificationTaskPayload](config)
	defer queue.Close()

	log.Println("Publishing tasks to Kafka...")

	// ── Publish task: Send Email ───────────────────────────────
	emailTask := usecase.NotificationTaskPayload{
		TaskType: usecase.SendEmailTask,
		Payload: usecase.SendEmailRequest{
			To:      "user@example.com",
			Subject: "Welcome!",
			Body:    "Thank you for registering.",
		},
	}

	if err := queue.Publish(ctx, "", emailTask); err != nil {
		log.Printf("failed to publish email task: %v", err)
	} else {
		log.Println("✓ email task published")
	}

	// ── Publish task: Send SMS ─────────────────────────────────
	smsTask := usecase.NotificationTaskPayload{
		TaskType: usecase.SendSMSTask,
		Payload: usecase.SendSMSRequest{
			PhoneNumber: "+628123456789",
			Message:     "Your OTP is 123456",
		},
	}

	if err := queue.Publish(ctx, "", smsTask); err != nil {
		log.Printf("failed to publish sms task: %v", err)
	} else {
		log.Println("✓ sms task published")
	}

	// ── Publish task: Generate Report ─────────────────────────
	reportTask := usecase.NotificationTaskPayload{
		TaskType: usecase.GenerateReportTask,
		Payload: usecase.GenerateReportRequest{
			ReportID:   "RPT-001",
			BusinessID: 42,
			ReportType: "monthly_summary",
		},
	}

	if err := queue.Publish(ctx, "", reportTask); err != nil {
		log.Printf("failed to publish report task: %v", err)
	} else {
		log.Println("✓ report task published")
	}

	time.Sleep(1 * time.Second) // wait a moment before closing
	log.Println("All tasks published. Producer finished.")
}
