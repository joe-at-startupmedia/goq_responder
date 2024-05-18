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

	mqs := MqRequester{
		requester,
		errRqst,
		logger,
	}

	return &mqs
}

func (mqs *MqRequester) Request(data []byte) error {
	return mqs.MqRqst.Write(DEFAULT_MSG_TYPE, data)
}

func (mqs *MqRequester) RequestUsingMqRequest(req *MqRequest) error {
	if !req.HasId() {
		req.SetId()
	}
	pbm := proto.Message(req.AsProtobuf())
	return mqs.RequestUsingProto(&pbm)
}

func (mqs *MqRequester) RequestUsingProto(req *proto.Message) error {
	data, err := proto.Marshal(*req)
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.Request(data)
}

func (mqs *MqRequester) WaitForResponse() ([]byte, error) {
	msg, err := mqs.MqRqst.Read()
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForResponse()
	} else {
		return msg.Data, err
	}
}

func (mqs *MqRequester) WaitForResponseTimed(duration time.Duration) ([]byte, error) {
	msg, err := mqs.MqRqst.ReadTimed(duration)
	if err != nil {
		return nil, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForResponseTimed(duration)
	} else if msg == ipc.TimeoutMessage {
		return nil, ipc.TimeoutMessage.Err
	} else {
		return msg.Data, err
	}
}

func (mqs *MqRequester) WaitForMqResponse() (*MqResponse, error) {
	mqResp := &protos.Response{}
	_, err := mqs.WaitForProto(mqResp)
	if err != nil {
		return nil, err
	}
	return ProtoResponseToMqResponse(mqResp), err
}

func (mqs *MqRequester) WaitForProto(pbm proto.Message) (*proto.Message, error) {

	msg, err := mqs.MqRqst.Read()

	if err != nil {
		return nil, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForProto(pbm)
	} else {
		err = proto.Unmarshal(msg.Data, pbm)
		return &pbm, err
	}
}

func (mqs *MqRequester) WaitForProtoTimed(pbm proto.Message, duration time.Duration) (*proto.Message, error) {

	msg, err := mqs.MqRqst.ReadTimed(duration)

	if err != nil {
		return nil, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_RECURSION_WAITTIME * time.Millisecond)
		return mqs.WaitForProtoTimed(pbm, duration)
	} else if msg == ipc.TimeoutMessage {
		return &pbm, ipc.TimeoutMessage.Err
	} else {
		err = proto.Unmarshal(msg.Data, pbm)
		return &pbm, err
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
