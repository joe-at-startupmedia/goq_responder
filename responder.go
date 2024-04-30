package goq_responder

import (
	"fmt"
	ipc "github.com/joe-at-startupmedia/golang-ipc"
	"log"
	"time"

	"github.com/joe-at-startupmedia/goq_responder/protos"
	"google.golang.org/protobuf/proto"
)

type ResponderCallback func(msq []byte) (processed []byte, err error)

type ResponderMqRequestCallback func(mqs *MqRequest) (mqr *MqResponse, err error)

type ResponderFromProtoMessageCallback func() (processed []byte, err error)

type MqResponder struct {
	MqRqst  *ipc.Server
	ErrRqst error
	MqResp  *ipc.Client
	ErrResp error
}

func NewResponder(config *QueueConfig) *MqResponder {

	requester, errRqst := ipc.StartServer(fmt.Sprintf("%s_rqst", config.Name), nil)

	mqr := MqResponder{
		requester,
		errRqst,
		&ipc.Client{Name: ""},
		nil,
	}

	return &mqr
}

func (mqr *MqResponder) StartClient(config *QueueConfig) *MqResponder {
	responder, errResp := ipc.StartClient(fmt.Sprintf("%s_resp", config.Name), nil)

	go func() {
		msg, err := responder.Read()
		if msg.MsgType == -1 {
			log.Println("MqResponder.StartClient status: ", responder.Status())
		}
		if err != nil {
			log.Println(fmt.Errorf("MqResponder.StartClient err: %w", err))
		}
	}()

	mqr.MqResp = responder
	mqr.ErrResp = errResp

	return mqr
}

// HandleMqRequest provides a concrete implementation of HandleRequestFromProto using the local MqRequest type
func (mqr *MqResponder) HandleMqRequest(requestProcessor ResponderMqRequestCallback) error {

	mqReq := &protos.Request{}

	return mqr.HandleRequestFromProto(mqReq, func() (processed []byte, err error) {

		mqResp, err := requestProcessor(ProtoRequestToMqRequest(mqReq))
		if err != nil {
			return nil, err
		}

		data, err := proto.Marshal(mqResp.AsProtobuf())

		if err != nil {
			return nil, fmt.Errorf("marshaling error: %w", err)
		}

		return data, nil
	})
}

// HandleRequestFromProto used to process arbitrary protobuf messages using a callback
func (mqr *MqResponder) HandleRequestFromProto(protocMsg proto.Message, msgHandler ResponderFromProtoMessageCallback) error {

	msg, err := mqr.MqRqst.Read()

	if err != nil {
		return err
	}

	if msg.MsgType < 1 {
		time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
		return mqr.HandleRequestFromProto(protocMsg, msgHandler)
	} else {

		err = proto.Unmarshal(msg.Data, protocMsg)
		if err != nil {
			return fmt.Errorf("unmarshaling error: %w", err)
		}

		processed, err := msgHandler()
		if err != nil {
			return err
		}

		return mqr.MqResp.Write(DEFAULT_MSG_TYPE, processed)
	}
}

func (mqr *MqResponder) HandleRequest(msgHandler ResponderCallback) error {
	return mqr.handleRequest(msgHandler, 0)
}

// HandleRequestWithLag used for testing purposes to simulate lagging responder
func (mqr *MqResponder) HandleRequestWithLag(msgHandler ResponderCallback, lag int) error {
	return mqr.handleRequest(msgHandler, lag)
}

func (mqr *MqResponder) handleRequest(msgHandler ResponderCallback, lag int) error {
	msg, err := mqr.MqRqst.Read()
	if err != nil {
		return err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
		return mqr.handleRequest(msgHandler, lag)
	} else {
		processed, err := msgHandler(msg.Data)
		if err != nil {
			return err
		}

		if lag > 0 {
			time.Sleep(time.Duration(lag) * time.Second)
		}

		err = mqr.MqResp.Write(DEFAULT_MSG_TYPE, processed)
		return err
	}
}

func (mqr *MqResponder) CloseResponder() error {
	mqr.MqRqst.Close()
	mqr.MqResp.Close()
	return nil
}

func (mqr *MqResponder) HasErrors() bool {
	return mqr.ErrResp != nil || mqr.ErrRqst != nil
}

func (mqr *MqResponder) Error() error {
	return fmt.Errorf("responder: %w\nrequester: %w", mqr.ErrResp, mqr.ErrRqst)
}

func CloseResponder(mqr *MqResponder) error {
	if mqr != nil {
		return mqr.CloseResponder()
	}
	return fmt.Errorf("pointer reference is nil")
}
