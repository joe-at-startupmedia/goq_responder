package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/goq_responder"
	"log"
	"time"
)

const maxRequestTickNum = 10

const queue_name = "goqr_example_mqrequest"

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(1 * time.Second)
	request_c := make(chan int)
	go requester(request_c)
	<-resp_c
	<-request_c

	goq_responder.CloseRequester(mqs)
	goq_responder.CloseResponder(mqr)
	//gives time for deferred functions to complete
	time.Sleep(2 * time.Second)
}

var mqr *goq_responder.MqResponder
var mqs *goq_responder.MqRequester
var config = goq_responder.QueueConfig{
	Name: queue_name,
}

func responder(c chan int) {

	mqr = goq_responder.NewResponder(&config)
	time.Sleep(5 * time.Second)
	defer func() {
		log.Println("Responder: finished")
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

	mqr.StartClient(&config)
	mqs.StartClient(&config)
	time.Sleep(5 * time.Second)

	if mqs.HasErrors() {
		log.Printf("Requester: could not initialize: %s", mqs.Error())
		c <- 1
		return
	}
	if mqr.HasErrors() {
		log.Printf("Responder: could not initialize: %s", mqr.Error())
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
			log.Printf("Requester: error requesting request: %s\n", err)
			continue
		}

		log.Printf("Requester: sent a new request: %s", request)

		msg, _, err := mqs.WaitForMqResponse(time.Second)

		if err != nil {
			log.Printf("Requester: error getting response: %s\n", err)
			continue
		}

		log.Printf("Requester: got a response: %s\n", msg.ValueStr)
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
