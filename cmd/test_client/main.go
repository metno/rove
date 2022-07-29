package main

import (
	"context"
	"fmt"
	"io"

	pb "github.com/metno/rove/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial(":50051", grpc.WithInsecure())
	if err != nil {
		panic(fmt.Sprintf("can not connect with server %v", err))
	}

	client := pb.NewCoordinatorClient(conn)

	in := pb.ValidateOneRequest{DataId: 1, Tests: []string{"test1"}}
	stream, err := client.ValidateOne(context.Background(), &in)
	if err != nil {
		panic(fmt.Sprintf("open stream error %v", err))
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(fmt.Sprintf("cannot receive %v", err))
		}
		fmt.Printf("Resp received: %d", resp.Flag)
	}

}
