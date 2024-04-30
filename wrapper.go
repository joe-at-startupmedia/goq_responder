package goq_responder

// QueueConfig is used to configure an instance of the message queue.
type QueueConfig struct {
	Name string
}

const (
	DEFAULT_MSG_TYPE          = 1
	REQUEST_REURSION_WAITTIME = 1
)
