package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-kafka/internal/usecase"
	"go-kafka/internal/workers"
	"go-kafka/pkg/taskqueue"
)

func main() {
	// ── Context with graceful shutdown ──────────────────────
	// When there is a SIGINT (Ctrl+C) or SIGTERM (from k8s/docker stop) signal,
	// the context will be cancelled and the worker will stop cleanly
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── Kafka Configuration ─────────────────────────────────────
	config := taskqueue.DefaultKafkaConfig(
		[]string{"localhost:8667"}, // broker address
		"notification-group",       // consumer group ID
	)
	config.DefaultTopic = "notification-topic"
	config.MaxRetries = 3

	// ── Queue Initialization ────────────────────────────────────
	queue := taskqueue.NewKafkaTaskQueue[usecase.NotificationTaskPayload](config)
	defer queue.Close()

	// ── Worker Initialization ───────────────────────────────────
	worker := workers.NewNotificationWorker(queue)

	// ── Run Worker ───────────────────────────────────────
	log.Println("Starting notification worker...")

	// Start() is blocking — it will keep running until the context is cancelled
	if err := worker.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker stopped with error: %v", err)
	}

	log.Println("Worker stopped gracefully")
}
