package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/goq_responder"
	"github.com/joe-at-startupmedia/posix_mq"
	"log"
	"time"
)

const maxRequestTickNum = 10

const queue_name = "goqr_example_mqrequest"

var owner = goq_responder.Ownership{
	//Username: "nobody", //uncomment to test ownership handling and errors
}

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(1 * time.Second)
	request_c := make(chan int)
	go requester(request_c)
	<-resp_c
	<-request_c
	//gives time for deferred functions to complete
	time.Sleep(2 * time.Second)
}

func responder(c chan int) {
	config := goq_responder.QueueConfig{
		Name:  queue_name,
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT,
	}
	mqr := goq_responder.NewResponder(&config, &owner)
	defer func() {
		goq_responder.UnlinkResponder(mqr)
		fmt.Println("Responder: finished and unlinked")
		c <- 0
	}()
	if mqr.HasErrors() {
		log.Printf("Responder: could not initialize: %s", mqr.Error())
		c <- 1
		return
	}

	count := 0
	for {
		time.Sleep(1 * time.Second)
		count++
		if err := mqr.HandleMqRequest(requestProcessor); err != nil {
			fmt.Printf("Responder: error handling request: %s\n", err)
			continue
		}

		fmt.Println("Responder: Sent a response")

		if count >= maxRequestTickNum {
			break
		}
	}
}

func requester(c chan int) {
	mqs := goq_responder.NewRequester(&goq_responder.QueueConfig{
		Name: queue_name,
	}, &owner)
	defer func() {
		goq_responder.CloseRequester(mqs)
		fmt.Println("Requester: finished and closed")
		c <- 0
	}()
	if mqs.HasErrors() {
		log.Printf("Requester: could not initialize: %s", mqs.Error())
		c <- 1
		return
	}

	count := 0
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		if err := mqs.RequestUsingMqRequest(&goq_responder.MqRequest{
			Arg1: request,
		}, 0); err != nil {
			fmt.Printf("Requester: error requesting request: %s\n", err)
			continue
		}

		fmt.Printf("Requester: sent a new request: %s", request)

		msg, _, err := mqs.WaitForMqResponse(time.Second)

		if err != nil {
			fmt.Printf("Requester: error getting response: %s\n", err)
			continue
		}

		fmt.Printf("Requester: got a response: %s\n", msg.ValueStr)
		//fmt.Printf("Requester: got a response: %-v\n", msg)

		if count >= maxRequestTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func requestProcessor(request *goq_responder.MqRequest) (*goq_responder.MqResponse, error) {
	response := goq_responder.MqResponse{}
	//assigns the response.request_id
	response.PrepareFromRequest(request)
	response.ValueStr = fmt.Sprintf("I recieved request: %s\n", request.Arg1)
	return &response, nil
}
