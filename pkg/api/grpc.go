package api

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/isnastish/kvs/pkg/log"
)

type GRPCServer struct {
	server *grpc.Server
}

type GrpcService interface {
	ServiceDesc() *grpc.ServiceDesc
}

///////////////////////////////////////////////////////////////////////////////////////
//Interceptors

type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	log.Logger.Info("====== [Server Stream Interceptor Wrapper] Received message")
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	log.Logger.Info("====== [Server Stream Interceptro Wrapper] Send message")
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{}
}

func transactionServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log.Logger.Info("====== [Server Stream Interceptor] %s", info.FullMethod)
	err := handler(srv, newWrappedStream(ss))
	if err != nil {
		log.Logger.Error("RPC failed with an error %v", err)
	}
	return err
}

///////////////////////////////////////////////////////////////////////////////////////

func NewGRPCServer(service GrpcService, opt ...grpc.ServerOption) *GRPCServer {
	server := &GRPCServer{
		server: grpc.NewServer(opt...),
	}

	// Could be put into a for loop for multiple services
	desc := service.ServiceDesc()
	server.server.RegisterService(desc, service)

	return server
}

func (s *GRPCServer) Serve(port uint) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Logger.Error("grpc: failed to listen %v", err)
		return fmt.Errorf("failed to listen %v", err)
	}

	log.Logger.Info("Listening on port 0.0.0.0:%d", port)

	err = s.server.Serve(listener)
	if err != nil {
		log.Logger.Error("Failed to serve %v", err)
	}

	return err
}

func (s *GRPCServer) Close() {
	s.server.Stop()
}
