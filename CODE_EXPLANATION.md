# Penjelasan Kode Project Go-Kafka

Project ini adalah implementasi sistem **Producer-Consumer** menggunakan **Apache Kafka** di Go. Sistem ini dirancang untuk menangani berbagai jenis tugas notifikasi (Email, SMS, dan Report) secara asynchronous.

## Struktur Direktori
```text
.
├── docker-compose.yml      # Konfigurasi Kafka & Zookeeper (Infrastructure)
├── cmd/
│   ├── main.go             # Entry point untuk Consumer (Worker)
│   └── producer.go         # Contoh implementasi Producer
├── internal/
│   ├── usecase/
│   │   └── notification.go # Definisi tipe task dan struktur payload
│   └── workers/
│   │   └── notification_worker.go # Logika pemrosesan pesan dari Kafka
└── pkg/
    └── taskqueue/          # Library/Abstraksi Messaging Queue
        ├── taskqueue.go    # Interface Generic untuk Task Queue
        └── kafka.go        # Implementasi Task Queue menggunakan Kafka
```

---

## 1. Abstraksi Messaging Queue (`pkg/taskqueue/`)

### `taskqueue.go`
File ini mendefinisikan interface `TaskQueue[T any]` yang bersifat generic.
*   **`Publish(ctx, topic, payload)`**: Digunakan oleh Producer untuk mengirim data ke antrean.
*   **`Start(ctx, topic, handler)`**: Digunakan oleh Consumer untuk mulai mendengarkan pesan dan memprosesnya menggunakan `handler`.
*   **`Close()`**: Menutup koneksi ke sistem messaging.

### `kafka.go`
Implementasi konkret dari `TaskQueue` menggunakan library `segmentio/kafka-go`.
*   **`KafkaConfig`**: Struktur untuk menyimpan konfigurasi koneksi (Brokers, GroupID, Retry, dll).
*   **`NewKafkaTaskQueue`**: Inisialisasi `kafka.Writer` untuk mengirim pesan.
*   **`Publish`**: Mengubah payload menjadi JSON dan mengirimnya ke Kafka dengan logika retry.
*   **`Start`**: 
    *   Inisialisasi `kafka.Reader`.
    *   **Manual Commit**: Pesan hanya di-commit setelah berhasil diproses untuk menjamin *at-least-once delivery*.
    *   Looping terus-menerus untuk mengambil pesan (`FetchMessage`).
    *   Logika **`runWithRetry`**: Jika handler gagal, sistem akan mencoba lagi sesuai konfigurasi sebelum lanjut ke pesan berikutnya.

---

## 2. Definisi Bisnis (`internal/usecase/`)

### `notification.go`
Berisi definisi data yang akan dikirim melalui Kafka.
*   **`TaskType`**: Enum untuk membedakan jenis tugas (`SEND_EMAIL`, `SEND_SMS`, `GENERATE_REPORT`).
*   **`NotificationTaskPayload`**: Wrapper utama yang dikirim ke Kafka. Menggunakan field `Payload any` agar fleksibel menampung berbagai struktur data.
*   **Request Structs**: Definisi spesifik data untuk Email (`SendEmailRequest`), SMS (`SendSMSRequest`), dan Report (`GenerateReportRequest`).

---

## 3. Pemrosesan Pesan (`internal/workers/`)

### `notification_worker.go`
Worker yang bertugas mengeksekusi logika bisnis saat pesan diterima.
*   **`Start`**: Mendaftarkan fungsi handler ke `taskqueue`.
*   **Switch Case**: Memeriksa `TaskType` dari payload, melakukan *unmarshal* ulang ke struct yang sesuai, lalu memanggil fungsi handler yang tepat.
*   **Handlers (`handleSendEmail`, dll)**: Simulasi proses (delay menggunakan `time.Sleep`) dan logging hasil eksekusi. Di sini tempat untuk menaruh integrasi nyata (seperti SMTP Client atau API Twilio).

---

## 4. Entry Points (`cmd/`)

### `main.go` (Consumer/Worker)
Aplikasi utama yang berjalan terus-menerus.
*   **Graceful Shutdown**: Menggunakan `signal.NotifyContext` agar aplikasi bisa berhenti dengan bersih (menutup koneksi Kafka) saat menerima sinyal `SIGINT` atau `SIGTERM`.
*   **Initialization**: Menyiapkan config, inisialisasi queue, dan menjalankan worker.

### `producer.go` (Producer Example)
Berisi fungsi `ProducerExample` yang menunjukkan cara mengirim 3 jenis tugas sekaligus ke Kafka.
*   Inisialisasi `KafkaTaskQueue`.
*   Membuat objek `NotificationTaskPayload`.
*   Memanggil `queue.Publish` untuk masing-masing tugas.

---

## 5. Infrastruktur (`docker-compose.yml`)
Menyediakan stack Kafka secara lokal menggunakan Docker.
*   **Zookeeper**: Dibutuhkan oleh Kafka untuk manajemen cluster.
*   **Kafka**: Berjalan pada port `8667` (internal: `9092`).
*   Menggunakan image dari `confluentinc/cp-kafka`.

---

## Alur Kerja Sistem
1.  **Infrastructure**: Jalankan Kafka menggunakan `docker-compose up -d`.
2.  **Consumer**: Jalankan `cmd/main.go`. Aplikasi akan menunggu pesan dari topik `notification-topic`.
3.  **Producer**: Saat Producer memanggil `Publish`, pesan JSON masuk ke Kafka.
4.  **Worker**: Consumer mengambil pesan, mengecek tipe task-nya, lalu mengeksekusi handler yang sesuai. Jika handler berhasil, offset Kafka diperbarui (Commit).
