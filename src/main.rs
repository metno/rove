use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{ValidateOneRequest, ValidateResponse};
use futures::Stream;
use std::pin::Pin;
use tonic::{transport::Server, Request, Response, Status};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ValidateResponse, Status>> + Send>>;

#[derive(Default)]
pub struct MyCoordinator {}

#[tonic::async_trait]
impl Coordinator for MyCoordinator {
    type ValidateOneStream = ResponseStream;

    async fn validate_one(
        &self,
        _req: Request<ValidateOneRequest>,
    ) -> Result<Response<Self::ValidateOneStream>, Status> {
        //
        Err(Status::unimplemented("not implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let coordinator = MyCoordinator::default();

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
