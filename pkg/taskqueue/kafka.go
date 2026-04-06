package taskqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaConfig stores Kafka connection configuration
type KafkaConfig struct {
	Brokers      []string
	GroupID      string
	DefaultTopic string

	// Consumer config
	MinBytes int // minimum bytes before fetch (default: 10KB)
	MaxBytes int // maximum bytes per fetch (default: 10MB)
	MaxWait  time.Duration

	// Retry config
	MaxRetries int
	RetryDelay time.Duration
}

func DefaultKafkaConfig(brokers []string, groupID string) KafkaConfig {
	return KafkaConfig{
		Brokers:    brokers,
		GroupID:    groupID,
		MinBytes:   10e3, // 10KB
		MaxBytes:   10e6, // 10MB
		MaxWait:    1 * time.Second,
		MaxRetries: 3,
		RetryDelay: 2 * time.Second,
	}
}

// KafkaTaskQueue is the implementation of TaskQueue using Kafka
type KafkaTaskQueue[T any] struct {
	config KafkaConfig
	writer *kafka.Writer
}

// NewKafkaTaskQueue creates a new instance of KafkaTaskQueue
func NewKafkaTaskQueue[T any](config KafkaConfig) *KafkaTaskQueue[T] {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll, // wait for all replicas to confirm
		Async:        false,            // synchronous write for data safety
	}

	return &KafkaTaskQueue[T]{
		config: config,
		writer: writer,
	}
}

// Publish sends the payload to the Kafka topic
func (k *KafkaTaskQueue[T]) Publish(ctx context.Context, topic string, payload T) error {
	if topic == "" {
		topic = k.config.DefaultTopic
	}

	// Serialize payload to JSON
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("taskqueue: failed to marshal payload: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Value: data,
		Time:  time.Now(),
	}

	// Retry logic when publishing fails
	var lastErr error
	for attempt := 0; attempt <= k.config.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("taskqueue: retrying publish (attempt %d/%d)", attempt, k.config.MaxRetries)
			time.Sleep(k.config.RetryDelay)
		}

		if err := k.writer.WriteMessages(ctx, msg); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return fmt.Errorf("taskqueue: failed to publish after %d retries: %w", k.config.MaxRetries, lastErr)
}

// Start starts the Kafka consumer and calls the handler for each message
func (k *KafkaTaskQueue[T]) Start(ctx context.Context, topic string, handler func(T) error) error {
	if topic == "" {
		topic = k.config.DefaultTopic
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  k.config.Brokers,
		GroupID:  k.config.GroupID,
		Topic:    topic,
		MinBytes: k.config.MinBytes,
		MaxBytes: k.config.MaxBytes,
		MaxWait:  k.config.MaxWait,

		// Do not auto-commit — we commit manually after the handler succeeds
		CommitInterval: 0,

		// Start from the latest message if no offset is stored
		StartOffset: kafka.LastOffset,

		// Error logger
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("[KAFKA ERROR] "+msg, args...)
		}),
	})

	defer reader.Close()

	log.Printf("taskqueue: consumer started. topic=%s, group=%s", topic, k.config.GroupID)

	for {
		// Check if context has been cancelled (shutdown signal)
		select {
		case <-ctx.Done():
			log.Println("taskqueue: context cancelled, stopping consumer")
			return ctx.Err()
		default:
		}

		// Fetch the next message from Kafka
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			// Context cancelled = normal shutdown
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("taskqueue: error fetching message: %v", err)
			continue
		}

		log.Printf("taskqueue: received message. topic=%s partition=%d offset=%d",
			msg.Topic, msg.Partition, msg.Offset)

		// Deserialize message
		var payload T
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			log.Printf("taskqueue: failed to unmarshal message: %v, skipping...", err)
			// Commit the message that failed to parse so it doesn't get stuck
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// Call the handler with retry
		handlerErr := k.runWithRetry(ctx, payload, handler)
		if handlerErr != nil {
			log.Printf("taskqueue: handler failed after retries: %v", handlerErr)
			// Still commit to avoid an infinite loop — send to DLQ in production
			// TODO: Implement Dead Letter Queue (DLQ) here
		}

		// Commit offset — mark the message as processed
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("taskqueue: failed to commit message offset: %v", err)
		}
	}
}

// runWithRetry runs the handler with retry if it fails
func (k *KafkaTaskQueue[T]) runWithRetry(ctx context.Context, payload T, handler func(T) error) error {
	var lastErr error

	for attempt := 0; attempt <= k.config.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("taskqueue: retrying handler (attempt %d/%d)", attempt, k.config.MaxRetries)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(k.config.RetryDelay * time.Duration(attempt)): // exponential-ish
			}
		}

		if err := handler(payload); err != nil {
			lastErr = err
			log.Printf("taskqueue: handler error: %v", err)
			continue
		}

		return nil
	}

	return fmt.Errorf("handler failed after %d attempts: %w", k.config.MaxRetries, lastErr)
}

// Close closes the writer
func (k *KafkaTaskQueue[T]) Close() error {
	return k.writer.Close()
}
