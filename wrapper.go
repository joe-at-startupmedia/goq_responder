package goq_responder

import (
	"errors"
	"fmt"
	"github.com/joe-at-startupmedia/posix_mq"
)

type QueueConfig posix_mq.QueueConfig

const (
	O_RDONLY = posix_mq.O_RDONLY
	O_WRONLY = posix_mq.O_WRONLY
	O_RDWR   = posix_mq.O_RDWR

	O_CLOEXEC  = posix_mq.O_CLOEXEC
	O_CREAT    = posix_mq.O_CREAT
	O_EXCL     = posix_mq.O_EXCL
	O_NONBLOCK = posix_mq.O_NONBLOCK
)

// GetFile gets the file on the OS where the queues are stored
func (config *QueueConfig) GetFile() string {
	return (*posix_mq.QueueConfig)(config).GetFile()
}

// NewMessageQueue returns an instance of the message queue given a QueueConfig.
func NewMessageQueue(config *QueueConfig) (*posix_mq.MessageQueue, error) {
	return posix_mq.NewMessageQueue((*posix_mq.QueueConfig)(config))
}

func NewMessageQueueWithOwnership(config QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {

	if len(postfix) > 0 {
		config.Name = fmt.Sprintf("%s_%s", config.Name, postfix)
	}

	var (
		messageQueue *posix_mq.MessageQueue
	)

	if config.Mode == 0 {
		if owner != nil && owner.IsValid() {
			config.Mode = 0660

		} else {
			config.Mode = 0666
		}
	}

	messageQueue, err := NewMessageQueue(&config)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Could not create message queue %s: %-v", config.GetFile(), err))
	}

	err = ApplyPermissions(owner, &config)
	if err != nil {
		return messageQueue, errors.New(fmt.Sprintf("Could not apply permissions %s: %-v", config.GetFile(), err))
	}

	return messageQueue, nil
}

type BidirectionalQueue struct {
	MqRqst  *posix_mq.MessageQueue
	ErrRqst error
	MqResp  *posix_mq.MessageQueue
	ErrResp error
}

func (bdr *BidirectionalQueue) Close() error {
	if err := bdr.MqRqst.Close(); err != nil {
		return err
	}
	return bdr.MqResp.Close()
}

func (bdr *BidirectionalQueue) Unlink() error {
	if err := bdr.MqRqst.Unlink(); err != nil {
		return err
	}
	return bdr.MqResp.Unlink()
}

func (bdr *BidirectionalQueue) HasErrors() bool {
	return bdr.ErrResp != nil || bdr.ErrRqst != nil
}

func (bdr *BidirectionalQueue) Error() error {
	return fmt.Errorf("responder: %w\nrequester: %w", bdr.ErrResp, bdr.ErrRqst)
}

func ForceRemoveQueue(queueFile string) error {
	return posix_mq.ForceRemoveQueue(queueFile)
}
