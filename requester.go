package goq_responder

import (
	"fmt"
	ipc "github.com/joe-at-startupmedia/golang-ipc"
	"github.com/joe-at-startupmedia/goq_responder/protos"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

type MqRequester struct {
	MqRqst  *ipc.Client
	ErrRqst error
	MqResp  *ipc.Server
	ErrResp error
}

func NewRequester(config *QueueConfig) *MqRequester {
	responder, errResp := ipc.StartServer(fmt.Sprintf("%s_resp", config.Name), nil)

	mqs := MqRequester{
		&ipc.Client{Name: ""},
		nil,
		responder,
		errResp,
	}

	return &mqs
}

func (mqs *MqRequester) StartClient(config *QueueConfig) *MqRequester {

	requester, errRqst := ipc.StartClient(fmt.Sprintf("%s_rqst", config.Name), nil)

	go func() {
		msg, err := requester.Read()
		if msg.MsgType < 1 {
			log.Println("MqRequest.StartClient status: ", requester.Status())
		}
		if err != nil {
			log.Println(fmt.Errorf("Request.StartClients err: %w", err))
		}
	}()

	mqs.MqRqst = requester
	mqs.ErrRqst = errRqst

	return mqs
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

func (mqs *MqRequester) WaitForResponse(duration time.Duration) ([]byte, uint, error) {
	msg, err := mqs.MqResp.Read()
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
		return mqs.WaitForResponse(duration)
	} else {
		return msg.Data, 0, err
	}
}

func (mqs *MqRequester) WaitForMqResponse(duration time.Duration) (*MqResponse, uint, error) {
	mqResp := &protos.Response{}
	_, prio, err := mqs.WaitForProto(mqResp, duration)
	if err != nil {
		return nil, 0, err
	}
	return ProtoResponseToMqResponse(mqResp), prio, err
}

func (mqs *MqRequester) WaitForProto(pbm proto.Message, duration time.Duration) (*proto.Message, uint, error) {
	msg, err := mqs.MqResp.Read()
	if err != nil {
		return nil, 0, err
	}
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
		return mqs.WaitForProto(pbm, duration)
	} else {
		err = proto.Unmarshal(msg.Data, pbm)
		return &pbm, 0, err
	}
}

func (mqs *MqRequester) CloseRequester() error {
	mqs.MqRqst.Close()
	mqs.MqResp.Close()
	return nil
}

func (mqs *MqRequester) HasErrors() bool {
	return mqs.ErrResp != nil || mqs.ErrRqst != nil
}

func (mqs *MqRequester) Error() error {
	return fmt.Errorf("responder: %w\nrequester: %w", mqs.ErrResp, mqs.ErrRqst)
}

func CloseRequester(mqs *MqRequester) error {
	if mqs != nil {
		return mqs.CloseRequester()
	}
	return fmt.Errorf("pointer reference is nil")
}
