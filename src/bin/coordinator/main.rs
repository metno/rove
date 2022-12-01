use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{ValidateOneRequest, ValidateResponse};
use futures::Stream;
use std::{pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ValidateResponse, Status>> + Send>>;

#[derive(Debug, Default)]
pub struct MyCoordinator {}

#[tonic::async_trait]
impl Coordinator for MyCoordinator {
    type ValidateOneStream = ResponseStream;

    #[tracing::instrument]
    async fn validate_one(
        &self,
        request: Request<ValidateOneRequest>,
    ) -> Result<Response<Self::ValidateOneStream>, Status> {
        tracing::info!("Got a request: {:?}", request);

        let req = request.into_inner();

        let mut stream = Box::pin(
            tokio_stream::iter(vec![
                ValidateResponse {
                    data_id: req.data_id,
                    flag_id: 1,
                    flag: 0,
                },
                ValidateResponse {
                    data_id: req.data_id,
                    flag_id: 2,
                    flag: 0,
                },
                ValidateResponse {
                    data_id: req.data_id,
                    flag_id: 3,
                    flag: 1,
                },
            ])
            .throttle(Duration::from_millis(200)),
        );

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Ok(item)).await {
                    // match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ValidateOneStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let addr = "[::1]:1337".parse()?;
    let coordinator = MyCoordinator::default();

    tracing::info!(message = "Starting server.", %addr);

    Server::builder()
        .trace_fn(|_| tracing::info_span!("helloworld_server"))
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
