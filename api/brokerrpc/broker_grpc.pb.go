// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: api/broker.proto

package brokerrpc

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	FeedBroker_StreamQuotes_FullMethodName = "/tickerfeed.FeedBroker/StreamQuotes"
)

// FeedBrokerClient is the client API for FeedBroker service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type FeedBrokerClient interface {
	StreamQuotes(ctx context.Context, opts ...grpc.CallOption) (FeedBroker_StreamQuotesClient, error)
}

type feedBrokerClient struct {
	cc grpc.ClientConnInterface
}

func NewFeedBrokerClient(cc grpc.ClientConnInterface) FeedBrokerClient {
	return &feedBrokerClient{cc}
}

func (c *feedBrokerClient) StreamQuotes(ctx context.Context, opts ...grpc.CallOption) (FeedBroker_StreamQuotesClient, error) {
	stream, err := c.cc.NewStream(ctx, &FeedBroker_ServiceDesc.Streams[0], FeedBroker_StreamQuotes_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &feedBrokerStreamQuotesClient{stream}
	return x, nil
}

type FeedBroker_StreamQuotesClient interface {
	Send(*StreamRequest) error
	Recv() (*Quotes, error)
	grpc.ClientStream
}

type feedBrokerStreamQuotesClient struct {
	grpc.ClientStream
}

func (x *feedBrokerStreamQuotesClient) Send(m *StreamRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *feedBrokerStreamQuotesClient) Recv() (*Quotes, error) {
	m := new(Quotes)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// FeedBrokerServer is the server API for FeedBroker service.
// All implementations must embed UnimplementedFeedBrokerServer
// for forward compatibility
type FeedBrokerServer interface {
	StreamQuotes(FeedBroker_StreamQuotesServer) error
	mustEmbedUnimplementedFeedBrokerServer()
}

// UnimplementedFeedBrokerServer must be embedded to have forward compatible implementations.
type UnimplementedFeedBrokerServer struct {
}

func (UnimplementedFeedBrokerServer) StreamQuotes(FeedBroker_StreamQuotesServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamQuotes not implemented")
}
func (UnimplementedFeedBrokerServer) mustEmbedUnimplementedFeedBrokerServer() {}

// UnsafeFeedBrokerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to FeedBrokerServer will
// result in compilation errors.
type UnsafeFeedBrokerServer interface {
	mustEmbedUnimplementedFeedBrokerServer()
}

func RegisterFeedBrokerServer(s grpc.ServiceRegistrar, srv FeedBrokerServer) {
	s.RegisterService(&FeedBroker_ServiceDesc, srv)
}

func _FeedBroker_StreamQuotes_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(FeedBrokerServer).StreamQuotes(&feedBrokerStreamQuotesServer{stream})
}

type FeedBroker_StreamQuotesServer interface {
	Send(*Quotes) error
	Recv() (*StreamRequest, error)
	grpc.ServerStream
}

type feedBrokerStreamQuotesServer struct {
	grpc.ServerStream
}

func (x *feedBrokerStreamQuotesServer) Send(m *Quotes) error {
	return x.ServerStream.SendMsg(m)
}

func (x *feedBrokerStreamQuotesServer) Recv() (*StreamRequest, error) {
	m := new(StreamRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// FeedBroker_ServiceDesc is the grpc.ServiceDesc for FeedBroker service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var FeedBroker_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "tickerfeed.FeedBroker",
	HandlerType: (*FeedBrokerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamQuotes",
			Handler:       _FeedBroker_StreamQuotes_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "api/broker.proto",
}
