# Dokumentasi Kode: Go-Kafka Producer-Consumer

Dokumen ini berisi kode sumber lengkap dan penjelasan teknis untuk setiap bagian dari proyek.

---

## 1. Abstraksi Task Queue (Interface)
**File:** `pkg/taskqueue/taskqueue.go`

```go
package taskqueue

import "context"

// TaskQueue is a generic interface for a task queue.
// T is the payload type to be sent/received.
type TaskQueue[T any] interface {
	// Publish mengirimkan payload ke antrean
	Publish(ctx context.Context, topic string, payload T) error

	// Start menjalankan consumer dan memanggil handler saat ada pesan masuk
	Start(ctx context.Context, topic string, handler func(payload T) error) error

	// Close menutup koneksi
	Close() error
}
```

**Penjelasan:**
*   **Generic `[T any]`**: Memungkinkan antrean ini digunakan untuk tipe data apa pun, sehingga kode lebih *reusable*.
*   **`Publish`**: Kontrak untuk mengirim data ke topik tertentu di Kafka.
*   **`Start`**: Kontrak untuk mulai mendengarkan pesan. Fungsi ini menerima `handler` (callback) yang akan dieksekusi setiap kali pesan diterima.

---

## 2. Implementasi Kafka
**File:** `pkg/taskqueue/kafka.go`

```go
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
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration

	// Retry config
	MaxRetries int
	RetryDelay time.Duration
}

// ... (DefaultKafkaConfig & NewKafkaTaskQueue)

func (k *KafkaTaskQueue[T]) Publish(ctx context.Context, topic string, payload T) error {
	if topic == "" {
		topic = k.config.DefaultTopic
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("taskqueue: failed to marshal payload: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Value: data,
		Time:  time.Now(),
	}

	var lastErr error
	for attempt := 0; attempt <= k.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(k.config.RetryDelay)
		}

		if err := k.writer.WriteMessages(ctx, msg); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to publish: %w", lastErr)
}

func (k *KafkaTaskQueue[T]) Start(ctx context.Context, topic string, handler func(T) error) error {
    // ... (Logika FetchMessage & Commit)
    // Lihat pkg/taskqueue/kafka.go untuk detail lengkap
}
```

**Penjelasan:**
*   **`NewKafkaTaskQueue`**: Menginisialisasi `kafka.Writer`. `Async: false` digunakan untuk memastikan data benar-benar terkirim sebelum fungsi mengembalikan nilai (keamanan data).
*   **Logika Retry**: Pada fungsi `Publish` dan `runWithRetry`, sistem akan mencoba mengirim ulang jika terjadi kegagalan jaringan sementara.
*   **Manual Commit**: Pesan diambil menggunakan `FetchMessage` dan dikomit secara manual (`CommitMessages`) hanya jika `handler` berhasil dijalankan. Ini mencegah hilangnya data jika aplikasi mati tiba-tiba.

---

## 3. Definisi Tugas & Payload
**File:** `internal/usecase/notification.go`

```go
package usecase

type TaskType string

const (
	SendEmailTask      TaskType = "SEND_EMAIL"
	SendSMSTask        TaskType = "SEND_SMS"
	GenerateReportTask TaskType = "GENERATE_REPORT"
)

type NotificationTaskPayload struct {
	TaskType TaskType `json:"task_type"`
	Payload  any      `json:"payload"`
}

type SendEmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// ... (SendSMSRequest & GenerateReportRequest)
```

**Penjelasan:**
*   **`TaskType`**: Bertindak sebagai identifier agar Consumer tahu cara memproses data tersebut.
*   **`Payload any`**: Menggunakan `any` (interface{}) agar kita bisa membungkus berbagai macam struktur data permintaan dalam satu pesan Kafka yang sama.

---

## 4. Logika Bisnis (Worker)
**File:** `internal/workers/notification_worker.go`

```go
func (w *NotificationWorker) Start(ctx context.Context) error {
	return w.queue.Start(ctx, "notification-topic", func(taskPayload usecase.NotificationTaskPayload) error {
		byteData, err := json.Marshal(taskPayload.Payload)
		if err != nil {
			return err
		}

		switch taskPayload.TaskType {
		case usecase.SendEmailTask:
			var req usecase.SendEmailRequest
			if err := json.Unmarshal(byteData, &req); err != nil {
				return err
			}
			return w.handleSendEmail(ctx, req)
        // ... (case lainnya: SendSMSTask, GenerateReportTask)
		}
        return nil
	})
}
```

**Penjelasan:**
*   **Dispatcher Pattern**: Fungsi `Start` bertindak sebagai dispatcher yang mendistribusikan pesan ke handler spesifik berdasarkan `TaskType`.
*   **Re-marshaling**: Karena `Payload` bertipe `any`, kita mengubahnya kembali ke byte lalu menguraikannya (`Unmarshal`) ke struct yang tepat agar bisa diproses oleh fungsi handler.

---

## 5. Aplikasi Utama (Consumer)
**File:** `cmd/main.go`

```go
func main() {
	// Menangkap sinyal berhenti (Ctrl+C)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	config := taskqueue.DefaultKafkaConfig(
		[]string{"localhost:8667"},
		"notification-group",
	)

	queue := taskqueue.NewKafkaTaskQueue[usecase.NotificationTaskPayload](config)
	defer queue.Close()

	worker := workers.NewNotificationWorker(queue)

	log.Println("Starting notification worker...")
	if err := worker.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker error: %v", err)
	}
}
```

**Penjelasan:**
*   **Graceful Shutdown**: `signal.NotifyContext` menangkap sinyal `Ctrl+C`. Saat sinyal diterima, `ctx.Done()` akan aktif, dan loop consumer di `kafka.go` akan berhenti secara elegan.
*   **Dependency Injection**: Kita menyuntikkan implementasi `queue` ke dalam `worker`, sehingga worker tidak perlu tahu detail teknis Kafka.

---

## 6. Contoh Pengiriman (Producer)
**File:** `cmd/producer.go`

```go
func ProducerExample() {
	// ... (Inisialisasi antrean)
	emailTask := usecase.NotificationTaskPayload{
		TaskType: usecase.SendEmailTask,
		Payload: usecase.SendEmailRequest{
			To:      "user@example.com",
			Subject: "Welcome!",
			Body:    "Thank you for registering.",
		},
	}

	queue.Publish(ctx, "", emailTask)
    // ... (Pengiriman task lainnya)
}
```

**Penjelasan:**
*   Producer hanya perlu membuat payload dan memanggil `Publish`.
*   Tidak ada logika bisnis berat di sisi Producer, hanya pengiriman pesan asynchronous untuk meningkatkan performa aplikasi utama.

---

## 7. Infrastruktur (Docker)
**File:** `docker-compose.yml`

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    # ...
  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "8667:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:8667
      # ...
```

**Penjelasan:**
*   Menjalankan Kafka dan Zookeeper dalam container. Port `8667` dipetakan agar aplikasi Go kita (yang berjalan di host) bisa terhubung ke broker Kafka di dalam container.
