package main

import (
	"errors"
	"fmt"
	"github.com/intarga/dagrid"
	pb "github.com/metno/rove/proto"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"time"
)

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

func constructSubDagIter(dag *dagrid.Dag, subdag *dagrid.Dag, curr_index int, nodes_visited map[int]int) {
	for child := range dag.Nodes[curr_index].Children {
		new_index, ok := nodes_visited[child]

		if !ok {
			new_index = subdag.Insert_child(nodes_visited[curr_index], dag.Nodes[child].Contents)
			nodes_visited[child] = new_index

			constructSubDagIter(dag, subdag, child, nodes_visited)
		} else {
			subdag.Add_edge(nodes_visited[curr_index], new_index)
		}
	}
}

// TODO: write a test for this
// TODO: maybe move this to package dagrid?
func constructSubDag(dag dagrid.Dag, required_nodes []string) (dagrid.Dag, error) {
	subdag := dagrid.New_dag()

	// nodes are put into the map when visited as [dag_index]subdag_index
	nodes_visited := make(map[int]int)

	for _, req := range required_nodes {
		index, ok := dag.IndexLookup[req]
		if !ok {
			return dagrid.Dag{}, errors.New("required test not found in dag")
		}

		_, ok = nodes_visited[index]
		if !ok {
			new_index := subdag.Insert_free_node(dag.Nodes[index].Contents)
			nodes_visited[index] = new_index

			constructSubDagIter(&dag, &subdag, index, nodes_visited)
		}
	}

	return subdag, nil
}

func runTestPlaceholder(test_name string, ch chan<- string) {
	time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)

	ch <- test_name
}

type server struct {
	pb.UnimplementedCoordinatorServer
	dag dagrid.Dag
}

func (s *server) ValidateOne(in *pb.ValidateOneRequest, srv pb.Coordinator_ValidateOneServer) error {
	subdag, err := constructSubDag(s.dag, in.Tests)

	return err
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
