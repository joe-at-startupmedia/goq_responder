package goq_responder

import (
	"github.com/google/uuid"
	"github.com/joe-at-startupmedia/goq_responder/protos"
)

type MqRequest protos.Request

func (mqr *MqRequest) HasId() bool {
	return len(mqr.Id) > 0
}

func (mqr *MqRequest) SetId() {
	mqr.Id = uuid.NewString()
}

// AsProtobuf used to convert the local type equivalent (MqRequest)
// back to its protobuf instance
func (mqr *MqRequest) AsProtobuf() *protos.Request {
	return (*protos.Request)(mqr)
}

// ProtoRequestToMqRequest used to convert the protobuf to the local
// type equivalent (MqRequest) for leveraging instance methods
func ProtoRequestToMqRequest(mqr *protos.Request) *MqRequest {
	return (*MqRequest)(mqr)
}
