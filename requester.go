package goq_responder

import (
	"fmt"
	ipc "github.com/james-barrow/golang-ipc"
	"github.com/joe-at-startupmedia/goq_responder/protos"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

type MqRequester struct {
	MqRqst  *ipc.Client
	ErrRqst error
}

func NewRequester(config *QueueConfig) *MqRequester {

	requester, errRqst := ipc.StartClient(fmt.Sprintf("%s", config.Name), &ipc.ClientConfig{
		Encryption: config.UseEncryption,
	})

	go func() {
		msg, err := requester.Read()
		if msg.MsgType < 1 {
			log.Println("MqRequest.StartClient status: ", requester.Status())
		}
		if err != nil {
			log.Println(fmt.Errorf("Request.StartClients err: %w", err))
		}
	}()

	mqs := MqRequester{
		requester,
		errRqst,
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

func (mqs *MqRequester) WaitForResponse(duration time.Duration) ([]byte, uint, error) {
	msg, err := mqs.MqRqst.Read()
	if msg.MsgType < 1 {
		time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
		return mqs.WaitForResponse(duration)
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
		time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
		return mqs.WaitForProto(pbm)
	} else {
		err = proto.Unmarshal(msg.Data, pbm)
		return &pbm, 0, err
	}
}

// DO NOT USE: needs more testing and refectoring
func (mqs *MqRequester) WaitForProtoTimed(pbm proto.Message, duration time.Duration) (*proto.Message, uint, error) {

	log.Print("WaitForProtoTimed")

	msgChan := make(chan *ipc.Message)
	errorChan := make(chan error)
	stop := make(chan int)

	go func() {
		for {
			fmt.Println("block")
			select {
			case <-time.After(time.Second * 5):
				fmt.Println("unblock")
				log.Print("timed out")
				err := fmt.Errorf("read operation timed out")
				errorChan <- err
			case <-stop:
				fmt.Println("stopped")
				return
			}
		}
	}()

	go func() {
		msg, err := mqs.MqRqst.Read()
		stop <- 1
		if err != nil {
			log.Print("WaitForProtoTimed error", err)
			errorChan <- err
		} else {
			msgChan <- msg
		}
	}()

	select {

	case err := <-errorChan:
		log.Print("WaitForProtoTimed errChan Met")
		return nil, 0, err
	case msg := <-msgChan:
		log.Print("msg Met", msg.Data)
		if msg.MsgType < 1 {
			time.Sleep(REQUEST_REURSION_WAITTIME * time.Second)
			return mqs.WaitForProto(pbm)
		} else {
			err := proto.Unmarshal(msg.Data, pbm)
			return &pbm, 0, err
		}
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
