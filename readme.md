# Go Kafka Task Queue Tutorial

A simple, robust notification system built with Go and Apache Kafka. This project demonstrates how to implement a generic task queue with manual offset commits, retry logic, and graceful shutdown.

## Features

- Generic Task Queue: Support for any serializable payload using Go Generics.
- Manual Commit: Ensures "at-least-once" delivery by committing only after successful processing.
- Retry Mechanism: Automatic retries for both publishing and processing tasks.
- Graceful Shutdown: Handles SIGINT and SIGTERM to stop workers cleanly.
- Kafka Monitoring: Built-in Kafka UI for easy debugging.

---

## Prerequisites

- Docker and Docker Compose
- Go 1.25+

---

## 1. Start Infrastructure

First, launch the Kafka broker, Zookeeper, and Kafka UI using Docker Compose:

```bash
docker-compose up -d
```

Check the status:
- Kafka Broker: localhost:8667
- Kafka UI: http://localhost:8666 (Monitor topics and messages here)

---

## 2. Project Structure

- cmd/main.go: The Consumer (Worker) entry point.
- cmd/producer/main.go: The Publisher (Producer) example.
- internal/workers/: Contains the worker logic and task handlers.
- internal/usecase/: Defines the task payloads and notification types.
- pkg/taskqueue/: Core Kafka wrapper implementation.

---

## 3. Run the Consumer (Worker)

Open a terminal and start the worker. It will wait for incoming tasks from the notification-topic.

```bash
go run cmd/main.go
```

The worker is designed to handle multiple task types:
- SendEmailTask
- SendSMSTask
- GenerateReportTask

---

## 4. Run the Producer (Publisher)

Open a new terminal and run the producer to send sample tasks to Kafka:

```bash
go run cmd/producer/main.go
```

The producer will publish three different types of notification tasks. You should see the worker processing them in the first terminal.

---

## How it Works

### Publishing a Task
The producer uses the taskqueue.NewKafkaTaskQueue with a specific payload type.

```go
queue := taskqueue.NewKafkaTaskQueue[usecase.NotificationTaskPayload](config)
err := queue.Publish(ctx, "topic-name", payload)
```

### Processing a Task
The worker starts a consumer and provides a handler function. The queue handles the JSON unmarshaling and retries automatically.

```go
err := queue.Start(ctx, "topic-name", func(task usecase.NotificationTaskPayload) error {
    // Process your logic here
    return nil // Return error to trigger retry
})
```

### Manual Commits and Retries
The pkg/taskqueue/kafka.go implementation uses reader.FetchMessage and reader.CommitMessages. If the handler returns an error, it will retry up to 3 times (with exponential backoff) before committing the message to prevent infinite loops.

---

## Cleanup

To stop and remove the Docker containers:

```bash
docker-compose down
```
