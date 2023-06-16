use crate::{cache, util::ListenerType};
use olympian::qc_tests::dip_check;
use runner_pb::runner_server::{Runner, RunnerServer};
use runner_pb::{RunTestRequest, RunTestResponse};
use tonic::{transport::Server, Request, Response, Status};
use util::Flag;

mod util {
    tonic::include_proto!("util");

    impl From<olympian::qc_tests::Flag> for Flag {
        fn from(item: olympian::qc_tests::Flag) -> Self {
            match item {
                olympian::qc_tests::Flag::Pass => Self::Pass,
                olympian::qc_tests::Flag::Fail => Self::Fail,
                olympian::qc_tests::Flag::Warn => Self::Warn,
                olympian::qc_tests::Flag::Inconclusive => Self::Inconclusive,
                olympian::qc_tests::Flag::Invalid => Self::Invalid,
                olympian::qc_tests::Flag::DataMissing => Self::DataMissing,
            }
        }
    }
}

mod runner_pb {
    tonic::include_proto!("runner");
}

#[derive(Debug, Default)]
struct MyRunner {}

#[tonic::async_trait]
impl Runner for MyRunner {
    #[tracing::instrument]
    async fn run_test(
        &self,
        request: Request<RunTestRequest>,
    ) -> Result<Response<RunTestResponse>, Status> {
        tracing::info!("Got a request: {:?}", request);

        let req = request.into_inner();

        let flag: Flag = match req.test.as_str() {
            "dip_check" => {
                let data = cache::get_timeseries_data(
                    req.series_id,
                    req.time
                        .ok_or_else(|| Status::invalid_argument("invalid timestamp"))?
                        .seconds,
                )
                .await
                .map_err(|err| Status::not_found(format!("data not found by cache: {}", err)))?;
                dip_check(data, 2., 3.).into() //TODO use actual test params
            }
            _ => {
                if req.test.starts_with("test") {
                    Flag::Inconclusive
                } else {
                    return Err(Status::invalid_argument("invalid test name"));
                }
            }
        };

        let response = RunTestResponse { flag: flag.into() };

        tracing::debug!("sending response");

        Ok(Response::new(response))
    }
}

pub async fn start_server(listener: ListenerType) -> Result<(), Box<dyn std::error::Error>> {
    match listener {
        ListenerType::Addr(addr) => {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .init();

            let runner = MyRunner::default();

            tracing::info!(message = "Starting server.", %addr);

            Server::builder()
                .trace_fn(|_| tracing::info_span!("helloworld_server"))
                .add_service(RunnerServer::new(runner))
                .serve(addr)
                .await?;
        }
        ListenerType::UnixListener(stream) => {
            let runner = MyRunner::default();

            Server::builder()
                .add_service(RunnerServer::new(runner))
                .serve_with_incoming(stream)
                .await?;
        }
    }

    Ok(())
}
