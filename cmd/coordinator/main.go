package main

import (
	"fmt"
	"github.com/intarga/dagrid"
	pb "github.com/metno/rove/proto"
	"google.golang.org/grpc"
	"log"
	"net"
)

type server struct {
	pb.UnimplementedCoordinatorServer
	dag dagrid.Dag
}

func (s *server) ValidateOne(in *pb.ValidateOneRequest, srv pb.Coordinator_ValidateOneServer) error {
	return nil
}

func constructDag() dagrid.Dag {
	dag := dagrid.New_dag()

	test1 := dag.Insert_free_node("test1")

	test2 := dag.Insert_child(test1, "test2")
	test3 := dag.Insert_child(test1, "test3")

	test4 := dag.Insert_child(test2, "test4")
	test5 := dag.Insert_child(test3, "test5")

	test6 := dag.Insert_child(test4, "test6")
	dag.Add_edge(test5, test6)

	return dag
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 50051))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServer(s, &server{dag: constructDag()})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
