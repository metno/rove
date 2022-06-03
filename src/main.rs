use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{ValidateOneRequest, ValidateResponse};
use daggy::Dag;
use futures::Stream;
use std::{pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

fn construct_test_dependency_dag() -> Dag<String, ()> {
    let mut dag = Dag::<String, ()>::new();

    let et1 = dag.add_node("end_test_1".to_string());
    let et2 = dag.add_node("end_test_2".to_string());

    let (_, dt1) = dag.add_child(et1, (), "dep_test1".to_string());
    let (_, dt2) = dag.add_child(et2, (), "dep_test2".to_string());

    let (_, dtc) = dag.add_child(dt1, (), "common_test".to_string());
    dag.add_edge(dt2, dtc, ()).unwrap();

    dag
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ValidateResponse, Status>> + Send>>;

pub struct MyCoordinator {
    test_dependency_dag: Dag<String, ()>,
}

impl MyCoordinator {
    fn new(test_dependency_dag: Dag<String, ()>) -> Self {
        MyCoordinator {
            test_dependency_dag,
        }
    }
}

#[tonic::async_trait]
impl Coordinator for MyCoordinator {
    type ValidateOneStream = ResponseStream;

    async fn validate_one(
        &self,
        req: Request<ValidateOneRequest>,
    ) -> Result<Response<Self::ValidateOneStream>, Status> {
        let inner_req = req.into_inner();

        let mut stream = Box::pin(
            tokio_stream::iter(vec![
                ValidateResponse {
                    data_id: inner_req.data_id,
                    flag_id: 1,
                    flag: 0,
                },
                ValidateResponse {
                    data_id: inner_req.data_id,
                    flag_id: 2,
                    flag: 0,
                },
                ValidateResponse {
                    data_id: inner_req.data_id,
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
                match tx.send(Result::<_, Status>::Ok(item)).await {
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
    let addr = "[::1]:50051".parse().unwrap();
    let coordinator = MyCoordinator::new(construct_test_dependency_dag());

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
