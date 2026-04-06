package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-kafka/internal/usecase"
	"go-kafka/pkg/taskqueue"

	"github.com/bytedance/gopkg/util/logger"
)

// NotificationWorker processes notification tasks from Kafka
type NotificationWorker struct {
	queue taskqueue.TaskQueue[usecase.NotificationTaskPayload]
}

func NewNotificationWorker(
	queue taskqueue.TaskQueue[usecase.NotificationTaskPayload],
) *NotificationWorker {
	return &NotificationWorker{
		queue: queue,
	}
}

// Start registers the worker to the queue and starts consuming messages
func (w *NotificationWorker) Start(ctx context.Context) error {
	return w.queue.Start(ctx, "notification-topic", func(taskPayload usecase.NotificationTaskPayload) error {
		// Re-marshal payload.Payload (type any) to []byte
		// so it can be unmarshaled into the correct struct
		byteData, err := json.Marshal(taskPayload.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal inner payload: %w", err)
		}

		switch taskPayload.TaskType {

		// ─── Case 1: Send Email ───────────────────────────────────
		case usecase.SendEmailTask:
			var req usecase.SendEmailRequest
			if err := json.Unmarshal(byteData, &req); err != nil {
				return fmt.Errorf("failed to unmarshal SendEmailRequest: %w", err)
			}
			return w.handleSendEmail(ctx, req)

		// ─── Case 2: Send SMS ─────────────────────────────────────
		case usecase.SendSMSTask:
			var req usecase.SendSMSRequest
			if err := json.Unmarshal(byteData, &req); err != nil {
				return fmt.Errorf("failed to unmarshal SendSMSRequest: %w", err)
			}
			return w.handleSendSMS(ctx, req)

		// ─── Case 3: Generate Report ──────────────────────────────
		case usecase.GenerateReportTask:
			var req usecase.GenerateReportRequest
			if err := json.Unmarshal(byteData, &req); err != nil {
				return fmt.Errorf("failed to unmarshal GenerateReportRequest: %w", err)
			}
			return w.handleGenerateReport(ctx, req)

		default:
			// Unknown task → do not retry, skip directly
			return errors.New("unknown task type: " + string(taskPayload.TaskType))
		}
	})
}

// ─────────────────────────────────────────────────────────────
// Handler per task type
// ─────────────────────────────────────────────────────────────

func (w *NotificationWorker) handleSendEmail(ctx context.Context, req usecase.SendEmailRequest) error {
	logger.Info(ctx, "handleSendEmail - processing", req)

	// Simulate sending email (replace with SMTP/SES/etc)
	time.Sleep(100 * time.Millisecond)

	if req.To == "" {
		return errors.New("email recipient is empty")
	}

	logger.Info(ctx, fmt.Sprintf("handleSendEmail - email sent to %s, subject: %s", req.To, req.Subject))
	return nil
}

func (w *NotificationWorker) handleSendSMS(ctx context.Context, req usecase.SendSMSRequest) error {
	logger.Info(ctx, "handleSendSMS - processing", req)

	// Simulate sending SMS (replace with Twilio/etc)
	time.Sleep(50 * time.Millisecond)

	if req.PhoneNumber == "" {
		return errors.New("phone number is empty")
	}

	logger.Info(ctx, fmt.Sprintf("handleSendSMS - SMS sent to %s", req.PhoneNumber))
	return nil
}

func (w *NotificationWorker) handleGenerateReport(ctx context.Context, req usecase.GenerateReportRequest) error {
	logger.Info(ctx, "handleGenerateReport - processing", req)

	// Simulate generating report (can be slow)
	time.Sleep(500 * time.Millisecond)

	logger.Info(ctx, fmt.Sprintf("handleGenerateReport - report generated. report_id=%s type=%s", req.ReportID, req.ReportType))
	return nil
}
