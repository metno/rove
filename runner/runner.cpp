#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "../proto/runner/runner.grpc.pb.h"

using grpc::Server;
using grpc::Status;
using grpc::ServerBuilder;
using grpc::ServerContext;

using runner::Runner;
using runner::RunTestRequest;
using runner::RunTestResponse;

// Logic and data behind the server's behavior.
class RunnerServiceImpl final : public Runner::Service {
  Status RunTest(ServerContext* context, const RunTestRequest* request,
                 RunTestResponse* reply) override {
    reply->set_flag_id(1);
    reply->set_flag(1);
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  RunnerServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
