package goq_responder

import (
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
)

// QueueConfig is used to configure an instance of the message queue.
type QueueConfig struct {
	Name                    string
	ServerUnmaskPermissions bool
	ClientRetryTimer        time.Duration
	ClientTimeout           time.Duration
	LogLevel                string
}

const (
	DEFAULT_MSG_TYPE            = 1
	REQUEST_RECURSION_WAITTIME  = 200
	DEFAULT_LOG_LEVEL           = logrus.ErrorLevel
	DEFAULT_CLIENT_CONNECT_WAIT = 100
)

func getLogrusLevel(logLevel string) logrus.Level {
	if os.Getenv("GOQ_DEBUG") == "true" {
		return logrus.DebugLevel
	} else {
		switch logLevel {
		case "debug":
			return logrus.DebugLevel
		case "info":
			return logrus.InfoLevel
		case "warn":
			return logrus.WarnLevel
		case "error":
			return logrus.ErrorLevel
		}
		return DEFAULT_LOG_LEVEL
	}
}

func InitLogging(ll string) *logrus.Logger {
	logger := logrus.New()
	logLevel := getLogrusLevel(ll)
	if logLevel > logrus.WarnLevel {
		logger.SetReportCaller(true)
	}
	logger.SetLevel(logLevel)
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
	})
	return logger
}

func GetDefaultClientConnectWait() int {
	envVar := os.Getenv("GOQ_CLIENT_CONNECT_WAIT")
	if len(envVar) > 0 {
		valInt, err := strconv.Atoi(envVar)
		if err == nil {
			return valInt
		}
	}
	return DEFAULT_CLIENT_CONNECT_WAIT
}

func Sleep() {
	waitTime := GetDefaultClientConnectWait()
	if waitTime > 5 {
		time.Sleep(time.Duration(waitTime) * time.Millisecond)
	} else {
		time.Sleep(time.Duration(waitTime) * time.Second)
	}
}
