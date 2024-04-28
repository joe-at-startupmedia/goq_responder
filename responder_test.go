package goq_responder_test

import (
	"fmt"
	"github.com/joe-at-startupmedia/goq_responder"
	"github.com/joe-at-startupmedia/posix_mq"
	"reflect"
	"syscall"
	"testing"
	"time"
)

const wired = "Narwhals and ice cream"

func TestRecvErrwithNonblocking(t *testing.T) {

	config := goq_responder.QueueConfig{
		Name:  "goq_testing_recverrwnblk",
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT | posix_mq.O_NONBLOCK,
	}
	mqr := goq_responder.NewResponder(&config, nil)
	assertNil(t, mqr.ErrResp)
	assertNil(t, mqr.ErrRqst)
	assertNotNil(t, mqr)

	msg, _, err := mqr.MqRqst.Receive()

	assertNil(t, msg)
	assertNotNil(t, err)
	assertEqual(t, syscall.EAGAIN, err.(syscall.Errno))
	err = mqr.UnlinkResponder()
	assertNil(t, err)
}

func TestRecvwithNonblocking(t *testing.T) {

	config := goq_responder.QueueConfig{
		Name:  "goq_testing_recvwnblk",
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT | posix_mq.O_NONBLOCK,
	}
	mqr := goq_responder.NewResponder(&config, nil)
	assertNil(t, mqr.ErrResp)
	assertNil(t, mqr.ErrRqst)
	assertNotNil(t, mqr)

	err := mqr.HandleRequest(func(request []byte) (processed []byte, err error) {
		return []byte(fmt.Sprintf("I recieved request: %s\n", request)), nil
	})
	assertNil(t, err)
	err = mqr.UnlinkResponder()
	assertNil(t, err)
}

func TestRecvErrwithBlocking(t *testing.T) {

	config := goq_responder.QueueConfig{
		Name:  "goq_testing_recverrwblk",
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT,
	}
	mqr := goq_responder.NewResponder(&config, nil)
	assertNil(t, mqr.ErrResp)
	assertNil(t, mqr.ErrRqst)
	assertNotNil(t, mqr)

	msg, _, err := mqr.MqRqst.TimedReceive(time.Second)

	assertNil(t, msg)
	assertNotNil(t, err)
	assertEqual(t, syscall.ETIMEDOUT, err.(syscall.Errno))
	err = mqr.UnlinkResponder()
	assertNil(t, err)
}

func assertNil(t *testing.T, i interface{}) {
	if !isNil(i) {
		t.Errorf("expected %-v to be nil", i)
	}
}

func assertNotNil(t *testing.T, i interface{}) {
	if isNil(i) {
		t.Errorf("expected %-v to not be nil", i)
	}
}

func assertEqual[T any](t *testing.T, ptr T, ptr2 T) {
	if !reflect.ValueOf(ptr).Equal(reflect.ValueOf(ptr2)) {
		t.Errorf("expected %-v to equal %-v", ptr, ptr2)
	}
}

// containsKind checks if a specified kind in the slice of kinds.
func containsKind(kinds []reflect.Kind, kind reflect.Kind) bool {
	for i := 0; i < len(kinds); i++ {
		if kind == kinds[i] {
			return true
		}
	}

	return false
}

// isNil checks if a specified object is nil or not, without Failing.
func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)
	kind := value.Kind()
	isNilableKind := containsKind(
		[]reflect.Kind{
			reflect.Chan, reflect.Func,
			reflect.Interface, reflect.Map,
			reflect.Ptr, reflect.Slice, reflect.UnsafePointer},
		kind)

	if isNilableKind && value.IsNil() {
		return true
	}

	return false
}
