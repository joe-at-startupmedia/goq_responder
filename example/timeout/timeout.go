package main

import (
	"errors"
	"fmt"
	ipc "github.com/joe-at-startupmedia/golang-ipc"
	"github.com/joe-at-startupmedia/goq_responder"
	"log"
	"time"
)

const maxRequestTickNum = 10

const queue_name = "goqr_example_timeout"

var mqr *goq_responder.MqResponder
var mqs *goq_responder.MqRequester
var config = goq_responder.QueueConfig{
	Name:             queue_name,
	ClientTimeout:    time.Duration(time.Second * 10),
	ClientRetryTimer: time.Duration(time.Second * 1),
}

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(time.Duration(goq_responder.GetDefaultClientConnectWait()) * time.Second)
	request_c := make(chan int)
	go requester(request_c)
	<-resp_c
	<-request_c

	goq_responder.CloseResponder(mqr)
	goq_responder.CloseRequester(mqs)
	//gives time for deferred functions to complete
	time.Sleep(2 * time.Second)
}

func responder(c chan int) {

	mqr = goq_responder.NewResponder(&config)

	defer func() {
		c <- 0
		log.Println("Responder: finished")
	}()
	if mqr.HasErrors() {
		log.Printf("Responder: could not initialize: %s", mqr.Error())
		c <- 1
		return
	}

	time.Sleep(time.Duration(goq_responder.GetDefaultClientConnectWait()) * time.Second)

	count := 0
	for {
		//time.Sleep(1 * time.Second)
		count++
		var err error
		if count > 5 {
			err = mqr.HandleRequestWithLag(handleMessage, count-4)
		} else {
			err = mqr.HandleRequest(handleMessage)
		}

		if err != nil {
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
		c <- 0
		log.Println("Requester: finished and closed")
	}()
	if mqs.HasErrors() {
		log.Printf("Requester: could not initialize: %s", mqs.Error())
		c <- 1
		return
	}
	time.Sleep(time.Duration(goq_responder.GetDefaultClientConnectWait()) * time.Second)

	count := 0
	ch := make(chan goqResponse, 13)
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
			log.Println(result[i].response)
		} else {
			log.Printf("Requester: Got error: %s \n", result[i].response)
		}
	}

}

func requestResponse(mqs *goq_responder.MqRequester, msg string, c chan goqResponse) {

	if len(msg) > 0 {
		err := mqs.Request([]byte(msg), 0)
		if err != nil {
			c <- goqResponse{fmt.Sprintf("%s", err), false}
			return
		}
		log.Printf("Requester: sent a new request: %s", msg)
	}

	resp, _, err := mqs.WaitForResponseTimed(time.Second * 5)

	if err != nil {

		if errors.Is(err, ipc.TimeoutMessage.Err) {
			log.Printf("Requester: requestResponse timedout, using recursion")
			go requestResponse(mqs, "", c)
			return
		}

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
