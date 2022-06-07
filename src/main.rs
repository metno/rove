use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{ValidateOneRequest, ValidateResponse};
use daggy::{Dag, NodeIndex, Walker};
use futures::Stream;
use std::{pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

fn construct_test_dependency_dag() -> (Dag<String, ()>, Vec<NodeIndex>) {
    let mut dag = Dag::<String, ()>::new();

    let et1 = dag.add_node("end_test_1".to_string());
    let et2 = dag.add_node("end_test_2".to_string());

    let roots = vec![et1, et2];

    let (_, dt1) = dag.add_child(et1, (), "dep_test1".to_string());
    let (_, dt2) = dag.add_child(et2, (), "dep_test2".to_string());

    let (_, dtc) = dag.add_child(dt1, (), "common_test".to_string());
    dag.add_edge(dt2, dtc, ()).unwrap();

    (dag, roots)
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ValidateResponse, Status>> + Send>>;

pub struct MyCoordinator {
    test_dependency_dag: Dag<String, ()>,
    roots: Vec<NodeIndex>,
}

impl MyCoordinator {
    fn new(test_dependency_dag: Dag<String, ()>, roots: Vec<NodeIndex>) -> Self {
        MyCoordinator {
            test_dependency_dag,
            roots,
        }
    }
}

fn add_children(
    dag: &Dag<String, ()>,
    curr_node: NodeIndex,
    new_dag: &mut Dag<String, ()>,
    curr_node_new: NodeIndex,
) {
    for (_, child) in dag.children(curr_node).iter(&dag) {
        let (_, new_child) = new_dag.add_child(
            curr_node_new,
            (),
            dag.node_weight(child).unwrap().to_string(),
        );
        add_children(dag, child, new_dag, new_child);
    }
}

fn reduce_dag(dag: &Dag<String, ()>, roots: Vec<NodeIndex>) -> Dag<String, ()> {
    match roots.len() {
        1 => {
            let root = *roots.first().unwrap();
            let mut new_dag = Dag::<String, ()>::new();
            let new_root = new_dag.add_node(dag.node_weight(root).unwrap().to_string());

            add_children(dag, root, &mut new_dag, new_root);

            new_dag
        }
        2 => dag.clone(),
        _ => panic!("Toy implementation only consider limited cases"),
    }
}

fn index_tests(
    tests: Vec<String>,
    dag: &Dag<String, ()>,
    roots: &Vec<NodeIndex>,
) -> Vec<NodeIndex> {
    let mut result = Vec::new();
    for test in tests {
        for index in roots {
            if dag.node_weight(*index).unwrap().to_string() == test {
                result.push(*index)
            }
        }
    }

    result
}

#[tonic::async_trait]
impl Coordinator for MyCoordinator {
    type ValidateOneStream = ResponseStream;

    async fn validate_one(
        &self,
        req: Request<ValidateOneRequest>,
    ) -> Result<Response<Self::ValidateOneStream>, Status> {
        let inner_req = req.into_inner();
        let needed_roots = index_tests(inner_req.tests, &self.test_dependency_dag, &self.roots);
        let _dag = reduce_dag(&self.test_dependency_dag, needed_roots);

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
    let (dag, roots) = construct_test_dependency_dag();
    let coordinator = MyCoordinator::new(dag, roots);

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
