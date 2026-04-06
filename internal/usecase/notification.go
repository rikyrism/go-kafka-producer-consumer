package usecase

// ==================== Task Types ====================

type TaskType string

const (
	SendEmailTask      TaskType = "SEND_EMAIL"
	SendSMSTask        TaskType = "SEND_SMS"
	GenerateReportTask TaskType = "GENERATE_REPORT"
)

// ==================== Task Payload ====================

// NotificationTaskPayload is the main wrapper sent to Kafka.
// TaskType determines which struct is inside the Payload.
type NotificationTaskPayload struct {
	TaskType TaskType `json:"task_type"`
	Payload  any      `json:"payload"`
}

// ==================== Email ====================

type SendEmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// ==================== SMS ====================

type SendSMSRequest struct {
	PhoneNumber string `json:"phone_number"`
	Message     string `json:"message"`
}

// ==================== Report ====================

type GenerateReportRequest struct {
	ReportID   string `json:"report_id"`
	BusinessID int64  `json:"business_id"`
	ReportType string `json:"report_type"`
}
