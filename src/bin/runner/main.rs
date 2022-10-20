use tonic::{transport::Server, Request, Response, Status};

use runner::runner_server::{Runner, RunnerServer};
use runner::{RunTestRequest, RunTestResponse};

pub mod runner {
    tonic::include_proto!("runner");
}

#[derive(Debug, Default)]
pub struct MyRunner {}

#[tonic::async_trait]
impl Runner for MyRunner {
    async fn run_test(
        &self,
        request: Request<RunTestRequest>,
    ) -> Result<Response<RunTestResponse>, Status> {
        println!("Got a request: {:?}", request);

        let response = RunTestResponse {
            flag: 0,
            flag_id: 0,
        };

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let runner = MyRunner::default();

    Server::builder()
        .add_service(RunnerServer::new(runner))
        .serve(addr)
        .await?;

    Ok(())
}
