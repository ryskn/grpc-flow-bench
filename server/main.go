package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	pb "github.com/ryskn/grpc-flow-bench/proto"
	"google.golang.org/grpc"
)

var addr = flag.String("addr", ":50051", "listen address")

type server struct {
	pb.UnimplementedBenchServer
}

func (s *server) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Seq:        req.Seq,
		ServerTsNs: time.Now().UnixNano(),
	}, nil
}

func (s *server) StreamPing(stream pb.Bench_StreamPingServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.PingResponse{
			Seq:        req.Seq,
			ServerTsNs: time.Now().UnixNano(),
		}); err != nil {
			return err
		}
	}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterBenchServer(s, &server{})
	fmt.Printf("server listening at %s\n", *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
