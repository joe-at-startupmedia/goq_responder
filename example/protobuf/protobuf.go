package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/joe-at-startupmedia/goq_responder"
	"github.com/joe-at-startupmedia/goq_responder/example/protobuf/protos"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

const maxRequestTickNum = 10

const queue_name = "goqr_example_protobuf"

var mqr *goq_responder.MqResponder
var mqs *goq_responder.MqRequester
var config = goq_responder.QueueConfig{
	Name:             queue_name,
	ClientTimeout:    0,
	ClientRetryTimer: 0,
}

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	goq_responder.Sleep()
	request_c := make(chan int)
	go requester(request_c)
	<-resp_c
	<-request_c

	goq_responder.CloseRequester(mqs)
	goq_responder.CloseResponder(mqr)
	//gives time for deferred functions to complete
	goq_responder.Sleep()
}

func responder(c chan int) {
	mqr = goq_responder.NewResponder(&config)
	defer func() {
		log.Println("Responder: finished")
		c <- 0
	}()

	if mqr.HasErrors() {
		log.Printf("Responder: could not initialize: %s", mqr.Error())
		c <- 1
		return
	}

	goq_responder.Sleep()

	count := 0
	for {
		//time.Sleep(1 * time.Second)
		count++
		if err := handleCmdRequest(mqr); err != nil {
			log.Printf("Responder: error handling request: %s\n", err)
			continue
		}

		log.Println("Responder: Sent a response")

		if count >= maxRequestTickNum {
			break
		}
	}
}

func requester(c chan int) {
	mqs = goq_responder.NewRequester(&config)
	defer func() {
		log.Println("Requester: finished and closed")
		c <- 0
	}()
	if mqs.HasErrors() {
		log.Printf("Requester: could not initialize: %s", mqs.Error())
		c <- 1
		return
	}
	goq_responder.Sleep()

	count := 0
	for {
		count++
		cmd := &protos.Cmd{
			Name: "restart",
			Arg1: fmt.Sprintf("%d", count), //using count as the id of the process
			ExecFlags: &protos.ExecFlags{
				User: "nonroot",
			},
		}
		if err := requestUsingCmd(mqs, cmd, 0); err != nil {
			log.Printf("Requester: error requesting request: %s\n", err)
			continue
		}

		log.Printf("Requester: sent a new request: %s \n", cmd.String())

		cmdResp, _, err := waitForCmdResponse(mqs, time.Second)

		if err != nil {
			log.Printf("Requester: error getting response: %s\n", err)
			continue
		}

		log.Printf("Requester: got a response: %s\n", cmdResp.ValueStr)

		if count >= maxRequestTickNum {
			break
		}

		//time.Sleep(1 * time.Second)
	}
}

func requestUsingCmd(mqs *goq_responder.MqRequester, req *protos.Cmd, priority uint) error {
	if len(req.Id) == 0 {
		req.Id = uuid.NewString()
	}
	pbm := proto.Message(req)
	return mqs.RequestUsingProto(&pbm, priority)
}

func waitForCmdResponse(mqs *goq_responder.MqRequester, duration time.Duration) (*protos.CmdResp, uint, error) {
	mqResp := &protos.CmdResp{}
	_, prio, err := mqs.WaitForProto(mqResp)
	if err != nil {
		return nil, 0, err
	}
	return mqResp, prio, err
}

// handleCmdRequest provides a concrete implementation of HandleRequestFromProto using the local Cmd protobuf type
func handleCmdRequest(mqr *goq_responder.MqResponder) error {

	cmd := &protos.Cmd{}

	return mqr.HandleRequestFromProto(cmd, func() (processed []byte, err error) {

		cmdResp := protos.CmdResp{}
		cmdResp.Id = cmd.Id
		cmdResp.ValueStr = fmt.Sprintf("I recieved request: %s(%s) - %s\n", cmd.Name, cmd.Id, cmd.Arg1)

		data, err := proto.Marshal(&cmdResp)
		if err != nil {
			return nil, fmt.Errorf("marshaling error: %w", err)
		}

		return data, nil
	})
}
