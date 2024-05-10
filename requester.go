package goq_responder

import (
	"fmt"
	ipc "github.com/joe-at-startupmedia/golang-ipc"
	"github.com/joe-at-startupmedia/goq_responder/protos"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"time"
)

type MqRequester struct {
	MqRqst  *ipc.Client
	ErrRqst error
	Logger  *logrus.Logger
}

func NewRequester(config *QueueConfig) *MqRequester {

	logger := InitLogging(config.LogLevel)

	requester, errRqst := ipc.StartClient(&ipc.ClientConfig{
		Name:       config.Name,
		LogLevel:   config.LogLevel,
		RetryTimer: config.ClientRetryTimer,
		Timeout:    config.ClientTimeout,
		Encryption: false,
	})

	/*
		go func() {
			msg, err := requester.Read()
			if msg.MsgType < 1 {
				logger.Debugf("MqRequest.StartClient status: %s", requester.Status())
			}
			if err != nil {
				logger.Errorf("Request.StartClients err: %s", err)
			}
		}()
	*/

	mqs := MqRequester{
		requester,
		errRqst,
		logger,
	}

	return &mqs
}

func (mqs *MqRequester) Request(data []byte, priority uint) error {
	return mqs.MqRqst.Write(DEFAULT_MSG_TYPE, data)
}

func (mqs *MqRequester) RequestUsingMqRequest(req *MqRequest, priority uint) error {
	if !req.HasId() {
		req.SetId()
	}
	pbm := proto.Message(req.AsProtobuf())
	return mqs.RequestUsingProto(&pbm, priority)
}

func (mqs *MqRequester) RequestUsingProto(req *proto.Message, priority uint) error {
	data, err := proto.Marshal(*req)
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.Request(data, priority)
}

func (mqs *MqRequester) WaitForResponse() ([]byte, uint, error) {
	msg, err := mqs.MqRqst.Read()
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForResponse()
	} else {
		return msg.Data, 0, err
	}
}

func (mqs *MqRequester) WaitForResponseTimed(duration time.Duration) ([]byte, uint, error) {
	msg, err := mqs.MqRqst.ReadTimed(duration, ipc.TimeoutMessage)
	if err != nil {
		return nil, 0, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForResponseTimed(duration)
	} else if msg == ipc.TimeoutMessage {
		return nil, 0, ipc.TimeoutMessage.Err
	} else {
		return msg.Data, 0, err
	}
}

func (mqs *MqRequester) WaitForMqResponse(duration time.Duration) (*MqResponse, uint, error) {
	mqResp := &protos.Response{}
	_, prio, err := mqs.WaitForProto(mqResp)
	if err != nil {
		return nil, 0, err
	}
	return ProtoResponseToMqResponse(mqResp), prio, err
}

func (mqs *MqRequester) WaitForProto(pbm proto.Message) (*proto.Message, uint, error) {

	msg, err := mqs.MqRqst.Read()

	if err != nil {
		return nil, 0, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForProto(pbm)
	} else {
		err = proto.Unmarshal(msg.Data, pbm)
		return &pbm, 0, err
	}
}

func (mqs *MqRequester) WaitForProtoTimed(pbm proto.Message, duration time.Duration) (*proto.Message, uint, error) {

	msg, err := mqs.MqRqst.ReadTimed(duration, ipc.TimeoutMessage)

	if err != nil {
		return nil, 0, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForProtoTimed(pbm, duration)
	} else if msg == ipc.TimeoutMessage {
		return &pbm, 0, ipc.TimeoutMessage.Err
	} else {
		err = proto.Unmarshal(msg.Data, pbm)
		return &pbm, 0, err
	}
}

func (mqs *MqRequester) CloseRequester() error {
	mqs.MqRqst.Close()
	return nil
}

func CloseRequester(mqs *MqRequester) error {
	if mqs != nil {
		return mqs.CloseRequester()
	}
	return fmt.Errorf("pointer reference is nil")
}

func (mqs *MqRequester) HasErrors() bool {
	return mqs.ErrRqst != nil
}

func (mqs *MqRequester) Error() error {
	return fmt.Errorf("%w", mqs.ErrRqst)
}
