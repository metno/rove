use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{ValidateOneRequest, ValidateResponse};
use dagmar::{Dag, NodeId};
use futures::Stream;
use prost_types::Timestamp;
use runner::{runner_client::RunnerClient, RunTestRequest, RunTestResponse};
use std::{collections::HashMap, error::Error, fmt::Display, pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub mod runner {
    tonic::include_proto!("runner");
}

#[derive(Debug)]
enum CoordinatorError {
    InvalidLookup,
}

impl Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "requested test not found in dag")
    }
}

impl Error for CoordinatorError {}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ValidateResponse, Status>> + Send>>;

async fn run_test(
    test_name: String,
    time: Timestamp,
) -> Result<(String, RunTestResponse), Box<dyn Error>> {
    let mut client = RunnerClient::connect("[::1]:1338").await?;

    Ok((
        test_name.clone(),
        client
            .run_test(tonic::Request::new(RunTestRequest {
                data_id: 1, // TODO: use an actual data_id
                time: Some(time),
                test: test_name,
            }))
            .await?
            .into_inner(),
    ))
}

#[derive(Debug)]
pub struct MyCoordinator {
    dag: Dag<String>,
}

impl MyCoordinator {
    fn new(dag: Dag<String>) -> Self {
        MyCoordinator { dag }
    }

    // TODO: write a test for this
    fn construct_subdag(
        &self,
        required_nodes: Vec<String>,
    ) -> Result<Dag<String>, CoordinatorError> {
        fn add_descendants(
            dag: &Dag<String>,
            subdag: &mut Dag<String>,
            curr_index: NodeId,
            nodes_visited: &mut HashMap<NodeId, NodeId>,
        ) {
            for child_index in dag.nodes.get(curr_index).unwrap().children.iter() {
                if let Some(new_index) = nodes_visited.get(child_index) {
                    subdag.add_edge(*nodes_visited.get(&curr_index).unwrap(), *new_index);
                } else {
                    let new_index =
                        subdag.add_node(dag.nodes.get(*child_index).unwrap().elem.clone());

                    nodes_visited.insert(*child_index, new_index);

                    add_descendants(dag, subdag, *child_index, nodes_visited);
                }
            }
        }

        let mut subdag = Dag::new();

        // this maps NodeIds from the dag to NodeIds from the subdag
        let mut nodes_visited: HashMap<NodeId, NodeId> = HashMap::new();

        for required in required_nodes.into_iter() {
            let index = self
                .dag
                .index_lookup
                .get(&required)
                .ok_or(CoordinatorError::InvalidLookup)?;

            if !nodes_visited.contains_key(index) {
                let subdag_index =
                    subdag.add_node(self.dag.nodes.get(*index).unwrap().elem.clone());

                nodes_visited.insert(*index, subdag_index);

                add_descendants(&self.dag, &mut subdag, *index, &mut nodes_visited);
            }
        }

        Ok(subdag)
    }
}

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

        let _subdag = self.construct_subdag(req.tests);

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

fn construct_dag_placeholder() -> Dag<String> {
    let mut dag: Dag<String> = Dag::new();

    let test6 = dag.add_node(String::from("test6"));

    let test4 = dag.add_node_with_children(String::from("test4"), vec![test6]);
    let test5 = dag.add_node_with_children(String::from("test5"), vec![test6]);

    let test2 = dag.add_node_with_children(String::from("test2"), vec![test4]);
    let test3 = dag.add_node_with_children(String::from("test3"), vec![test5]);

    let _test1 = dag.add_node_with_children(String::from("testl"), vec![test2, test3]);

    dag
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let addr = "[::1]:1337".parse()?;
    let coordinator = MyCoordinator::new(construct_dag_placeholder());

    tracing::info!(message = "Starting server.", %addr);

    Server::builder()
        .trace_fn(|_| tracing::info_span!("helloworld_server"))
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
