#!/bin/bash

mkdir proto

python3 -m grpc_tools.protoc -I../rove --python_out=. --pyi_out=. --grpc_python_out=. proto/rove.proto
