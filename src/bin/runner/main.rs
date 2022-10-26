use rove::cache;
use runner::runner_server::{Runner, RunnerServer};
use runner::{RunTestRequest, RunTestResponse};
use titanlib_rs::qc_tests::{dip_check, DipResult};
use tonic::{transport::Server, Request, Response, Status};

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

        let data = cache::get_timeseries_data(1, std::time::SystemTime::now()); //TODO use actual arguments
        let result = dip_check(data, 2., 3.); // TODO load actual params

        let flag = match result {
            // TODO derived integer conversion?
            DipResult::Pass => 0,
            _ => 1,
        };

        let response = RunTestResponse { flag, flag_id: 0 };

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
