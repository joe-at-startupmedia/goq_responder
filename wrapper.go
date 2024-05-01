package goq_responder

import (
	"github.com/sirupsen/logrus"
	"os"
)

// QueueConfig is used to configure an instance of the message queue.
type QueueConfig struct {
	Name              string
	UseEncryption     bool
	UnmaskPermissions bool
}

const (
	DEFAULT_MSG_TYPE          = 1
	REQUEST_REURSION_WAITTIME = 1
)

var Log *logrus.Logger

func InitLogging() {
	Log = logrus.New()
	if os.Getenv("GOQ_DEBUG") == "true" {
		Log.SetLevel(logrus.DebugLevel)
		Log.SetReportCaller(true)
	} else {
		Log.SetLevel(logrus.InfoLevel)
	}
	Log.SetOutput(os.Stdout)
	Log.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
	})
}
