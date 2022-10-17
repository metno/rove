#!/bin/bash

protoc --go_out=./golang --go_opt=paths=source_relative --go-grpc_out=./golang --go-grpc_opt=paths=source_relative proto/coordinator/coordinator.proto
protoc --go_out=./golang --go_opt=paths=source_relative --go-grpc_out=./golang --go-grpc_opt=paths=source_relative proto/runner/runner.proto
