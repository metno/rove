#!/bin/bash

protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/coordinator/coordinator.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/runner/runner.proto
protoc -I ./proto/runner --grpc_out=./proto/runner --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) ./proto/runner/runner.proto
protoc -I ./proto/runner --cpp_out=./proto/runner ./proto/runner/runner.proto
