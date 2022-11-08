package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/intarga/dagrid"
	pb_coordinator "github.com/metno/rove/proto/coordinator"
	pb_runner "github.com/metno/rove/proto/runner"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"math/rand"
	"net"
	"time"
)

type testResp struct {
	name string
	resp *pb_runner.RunTestResponse
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

func runTest(test_name string, ch chan<- testResp) {
	conn, err := grpc.Dial("localhost:1338")
	if err != nil {
		log.Fatalf("connection to runner failed: %v", err)
	}
	client := pb_runner.NewRunnerClient(conn)

	req := pb_runner.RunTestRequest{
		DataId: 1,
		Test:   test_name,
		Time:   timestamppb.Now(), // TODO replace with actual timestamp
	}

	resp, err := client.RunTest(context.Background(), &req)

	ch <- testResp{name: test_name, resp: resp}
}

type server struct {
	pb_coordinator.UnimplementedCoordinatorServer
	dag dagrid.Dag
}

func (s *server) ValidateOne(in *pb_coordinator.ValidateOneRequest, srv pb_coordinator.Coordinator_ValidateOneServer) error {
	subdag, err := constructSubDag(s.dag, in.Tests)
	nodes_left := len(subdag.Nodes) // warning: this assumes no nodes were removed from the dag

	// how many children of each node have been run
	// form: children_completed_map[node_index]children_completed
	children_completed_map := make(map[int]int)

	ch := make(chan testResp)

	for leaf_index := range subdag.Leaves {
		go runTest(subdag.Nodes[leaf_index].Contents, ch)
	}

	for completed_test := range ch {
		nodes_left--

		// TODO: send real data back to the client
		srv.Send(&pb_coordinator.ValidateResponse{DataId: in.DataId, FlagId: uint32(s.dag.IndexLookup[completed_test.name]), Flag: completed_test.resp.Flag}) // FIXME: is this FlagId correct? or should we use the one from the resp?

		if nodes_left == 0 {
			return nil
		}

		completed_index := subdag.IndexLookup[completed_test.name]

		for parent_index := range subdag.Nodes[completed_index].Parents {
			// TODO: think the contents of this loop can be simplified
			children_completed, ok := children_completed_map[parent_index]
			if !ok { // FIXME: is this necessary? default value of int should be 0 anyway
				children_completed = 0
			}

			children_completed++
			children_completed_map[parent_index] = children_completed

			if children_completed >= len(subdag.Nodes[parent_index].Children) {
				go runTest(subdag.Nodes[parent_index].Contents, ch)
			}
		}

	}

	return err
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 50051))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb_coordinator.RegisterCoordinatorServer(s, &server{dag: constructDag()})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
