use crate::util::{flag_int_to_string, ListenerType};
use chrono::Local;
use coordinator_pb::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator_pb::{ValidateOneRequest, ValidateResponse};
use dagmar::{Dag, NodeId};
use futures::{stream::FuturesUnordered, Stream};
use prost_types::Timestamp;
use runner_pb::{runner_client::RunnerClient, RunTestRequest, RunTestResponse};
use std::{collections::HashMap, error::Error, fmt::Display, pin::Pin, sync::Arc};
use tempfile::TempPath;
use tokio::{net::UnixStream, sync::mpsc};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{
    transport::{Endpoint, Server},
    Request, Response, Status,
};
use tower::service_fn;

mod util {
    tonic::include_proto!("util");
}

mod coordinator_pb {
    tonic::include_proto!("coordinator");
}

mod runner_pb {
    tonic::include_proto!("runner");
}

#[derive(Debug)]
enum CoordinatorError {
    InvalidLookup,
    RunnerConnectionFail,
    RunTestFail,
}

impl Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::InvalidLookup => write!(f, "requested test not found in dag"),
            Self::RunnerConnectionFail => write!(f, "failed to connect to runner"),
            Self::RunTestFail => write!(f, "RunTest request failed"),
        }
    }
}

impl Error for CoordinatorError {}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ValidateResponse, Status>> + Send>>;

#[derive(Debug, Clone)]
pub enum EndpointType {
    Uri(Endpoint),
    Socket(Arc<TempPath>),
}

// TODO: keep internal error when mapping errors?
async fn run_test(
    endpoint: EndpointType,
    test_name: String,
    time: Timestamp,
) -> Result<(String, RunTestResponse), CoordinatorError> {
    println!(
        "{} \x1b[1;31mCOORDINATOR\x1b[0m: Sending test \"{}\" to runner",
        Local::now(),
        test_name
    );
    let mut client = match endpoint {
        EndpointType::Uri(endpt) => RunnerClient::connect(endpt)
            .await
            .map_err(|_err| CoordinatorError::RunnerConnectionFail)?,
        EndpointType::Socket(socket) => {
            let channel = Endpoint::try_from("http://any.url")
                .unwrap()
                .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
                    let socket = Arc::clone(&socket);
                    async move { UnixStream::connect(&*socket).await }
                }))
                .await
                .unwrap();
            RunnerClient::new(channel)
        }
    };

    Ok((
        test_name.clone(),
        client
            .run_test(tonic::Request::new(RunTestRequest {
                data_id: 1, // TODO: use an actual data_id
                time: Some(time),
                test: test_name,
            }))
            .await
            .map_err(|_err| CoordinatorError::RunTestFail)?
            .into_inner(),
    ))
}

#[derive(Debug)]
struct MyCoordinator {
    dag: Dag<String>,
    runner_endpoint: EndpointType,
}

impl MyCoordinator {
    fn new(dag: Dag<String>, runner_endpoint: EndpointType) -> Self {
        MyCoordinator {
            dag,
            runner_endpoint,
        }
    }

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
                    subdag.add_edge(*nodes_visited.get(&curr_index).unwrap(), new_index);

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

        println!(
            "{} \x1b[1;31mCOORDINATOR\x1b[0m: Received ValidateOneRequest to run tests {:?} on data {}",
            Local::now(),
            req.tests,
            req.series_id
        );

        println!(
            "{} \x1b[1;31mCOORDINATOR\x1b[0m: Constructing subdag",
            Local::now(),
        );

        // TODO: remove this unwrap
        let subdag = self.construct_subdag(req.tests).unwrap();

        let endpoint = self.runner_endpoint.clone();

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            let mut children_completed_map: HashMap<NodeId, usize> = HashMap::new();
            let mut test_futures = FuturesUnordered::new();

            println!(
                "{} \x1b[1;31mCOORDINATOR\x1b[0m: Finding tests in subdag with no dependencies",
                Local::now(),
            );
            for leaf_index in subdag.leaves.clone().into_iter() {
                println!(
                            "{} \x1b[1;31mCOORDINATOR\x1b[0m: Found test \"{}\", with no dependencies, triggering a run",
                            Local::now(),
                            subdag.nodes[leaf_index].elem,
                        );
                test_futures.push(run_test(
                    endpoint.clone(),
                    subdag.nodes.get(leaf_index).unwrap().elem.clone(),
                    req.time.as_ref().unwrap().clone(),
                ));
            }

            while let Some(res) = test_futures.next().await {
                // TODO: remove this unwrap
                let unwrapped = res.unwrap();
                let validate_response = ValidateResponse {
                    series_id: req.series_id.clone(),
                    time: req.time.clone(),
                    test: unwrapped.0.clone(),
                    flag: unwrapped.1.flag,
                };

                println!(
                    "{} \x1b[1;31mCOORDINATOR\x1b[0m: Forwarding response from runner, test: {}, flag: {}",
                    Local::now(),
                    validate_response.test,
                    flag_int_to_string(validate_response.flag)
                );

                match tx.send(Ok(validate_response.clone())).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }

                let completed_index = subdag.index_lookup.get(&unwrapped.0).unwrap();

                println!(
                    "{} \x1b[1;31mCOORDINATOR\x1b[0m: Checking dependents of test \"{}\"",
                    Local::now(),
                    validate_response.test,
                );
                for parent_index in subdag.nodes.get(*completed_index).unwrap().parents.iter() {
                    let children_completed = children_completed_map
                        .get(parent_index)
                        .map(|x| x + 1)
                        .unwrap_or(1);

                    children_completed_map.insert(*parent_index, children_completed);

                    if children_completed >= subdag.nodes.get(*parent_index).unwrap().children.len()
                    {
                        println!(
                            "{} \x1b[1;31mCOORDINATOR\x1b[0m: Found dependent \"{}\" of test \"{}\", with no dependencies left, triggering a run",
                            Local::now(),
                            subdag.nodes[*parent_index].elem,
                            validate_response.test,
                        );
                        test_futures.push(run_test(
                            endpoint.clone(),
                            subdag.nodes.get(*parent_index).unwrap().elem.clone(),
                            req.time.as_ref().unwrap().clone(),
                        ))
                    } else {
                        println!(
                            "{} \x1b[1;31mCOORDINATOR\x1b[0m: Found dependent \"{}\" of test \"{}\", with {} dependencies left, cant run yet",
                            Local::now(),
                            subdag.nodes[*parent_index].elem,
                            validate_response.test,
                            children_completed
                        );
                    }
                }
            }
            println!(
                "{} \x1b[1;31mCOORDINATOR\x1b[0m: All tests in subdag completed!",
                Local::now(),
            );
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

    let _test1 = dag.add_node_with_children(String::from("test1"), vec![test2, test3]);

    dag
}

pub async fn start_server(
    listener: ListenerType,
    runner_endpoint: EndpointType,
) -> Result<(), Box<dyn std::error::Error>> {
    match listener {
        ListenerType::Addr(addr) => {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .init();

            let coordinator = MyCoordinator::new(construct_dag_placeholder(), runner_endpoint);

            tracing::info!(message = "Starting server.", %addr);

            Server::builder()
                .trace_fn(|_| tracing::info_span!("helloworld_server"))
                .add_service(CoordinatorServer::new(coordinator))
                .serve(addr)
                .await?;
        }
        ListenerType::UnixListener(stream) => {
            let coordinator = MyCoordinator::new(construct_dag_placeholder(), runner_endpoint);

            println!(
                "{} \x1b[1;31mCOORDINATOR\x1b[0m: Starting server",
                Local::now()
            );

            Server::builder()
                .add_service(CoordinatorServer::new(coordinator))
                .serve_with_incoming(stream)
                .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_subdag() {
        let coordinator = MyCoordinator::new(
            construct_dag_placeholder(),
            EndpointType::Uri(Endpoint::try_from("http://any.url").unwrap()),
        );

        assert_eq!(coordinator.dag.count_edges(), 6);

        let subdag = coordinator
            .construct_subdag(vec![String::from("test4")])
            .unwrap();

        assert_eq!(subdag.count_edges(), 1);

        let subdag = coordinator
            .construct_subdag(vec![String::from("test1")])
            .unwrap();

        assert_eq!(subdag.count_edges(), 6);
    }
}
