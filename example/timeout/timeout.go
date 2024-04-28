package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/goq_responder"
	"log"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

const maxRequestTickNum = 10

const queue_name = "goqr_example_timeout"

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
	mqr := goq_responder.NewResponder(&config, nil)
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
		var err error
		if count > 5 {
			err = mqr.HandleRequestWithLag(handleMessage, count-4)
		} else {
			err = mqr.HandleRequest(handleMessage)
		}

		if err != nil {
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
	}, nil)
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
	ch := make(chan goqResponse)
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		go requestResponse(mqs, request, ch)

		if count >= maxRequestTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}

	result := make([]goqResponse, maxRequestTickNum)
	for i := range result {
		result[i] = <-ch
		if result[i].status {
			fmt.Println(result[i].response)
		} else {
			fmt.Printf("Requester: Got error: %s \n", result[i].response)
		}
	}
}

func requestResponse(mqs *goq_responder.MqRequester, msg string, c chan goqResponse) {
	if err := mqs.Request([]byte(msg), 0); err != nil {
		c <- goqResponse{fmt.Sprintf("%s", err), false}
		return
	}
	fmt.Printf("Requester: sent a new request: %s", msg)

	resp, _, err := mqs.WaitForResponse(time.Second)

	if err != nil {
		c <- goqResponse{fmt.Sprintf("%s", err), false}
		return
	}

	c <- goqResponse{fmt.Sprintf("Requester: got a response: %s\n", resp), true}
}

type goqResponse struct {
	response string
	status   bool
}

func handleMessage(request []byte) (processed []byte, err error) {
	return []byte(fmt.Sprintf("I recieved request: %s\n", request)), nil
}
