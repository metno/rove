use crate::{
    data_switch::{DataSwitch, GeoPoint, SeriesCache, SpatialCache, Timerange, Timestamp},
    harness,
    // TODO: rethink this dependency?
    pb::{ValidateSeriesResponse, ValidateSpatialResponse},
};
use dagmar::{Dag, NodeId};
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_stream::StreamExt;
// TODO: remove
use tonic::Status;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in dag")]
    TestNotInDag(String),
    // #[error("failed to run test")]
    // Runner(#[from] runner::Error),
}

#[derive(Debug)]
pub struct Scheduler<'a> {
    // TODO: separate DAGs for series and spatial tests?
    dag: Dag<&'static str>,
    data_switch: DataSwitch<'a>,
}

impl<'a> Scheduler<'a> {
    pub fn new(dag: Dag<&'static str>, data_switch: DataSwitch<'a>) -> Self {
        Scheduler { dag, data_switch }
    }

    fn construct_subdag(
        &self,
        required_nodes: Vec<impl AsRef<str>>,
    ) -> Result<Dag<&'static str>, Error> {
        fn add_descendants(
            dag: &Dag<&'static str>,
            subdag: &mut Dag<&'static str>,
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
                .get(required.as_ref())
                .ok_or(Error::TestNotInDag(required.as_ref().to_string()))?;

            if !nodes_visited.contains_key(index) {
                let subdag_index =
                    subdag.add_node(self.dag.nodes.get(*index).unwrap().elem.clone());

                nodes_visited.insert(*index, subdag_index);

                add_descendants(&self.dag, &mut subdag, *index, &mut nodes_visited);
            }
        }

        Ok(subdag)
    }

    fn schedule_tests_series(
        subdag: Dag<&'static str>,
        data: SeriesCache,
    ) -> Receiver<Result<ValidateSeriesResponse, Status>> {
        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = channel(subdag.nodes.len());
        tokio::spawn(async move {
            let mut children_completed_map: HashMap<NodeId, usize> = HashMap::new();
            let mut test_futures = FuturesUnordered::new();

            for leaf_index in subdag.leaves.clone().into_iter() {
                test_futures.push(harness::run_test_series(
                    subdag.nodes.get(leaf_index).unwrap().elem,
                    &data,
                ));
            }

            while let Some(res) = test_futures.next().await {
                match tx
                    .send(
                        res.clone()
                            .map_err(|e| Status::aborted(format!("a test run failed: {}", e))),
                    )
                    .await
                {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }

                match res {
                    Ok(inner) => {
                        let completed_index = subdag.index_lookup.get(inner.test.as_str()).unwrap();

                        for parent_index in
                            subdag.nodes.get(*completed_index).unwrap().parents.iter()
                        {
                            let children_completed = children_completed_map
                                .get(parent_index)
                                .map(|x| x + 1)
                                .unwrap_or(1);

                            children_completed_map.insert(*parent_index, children_completed);

                            if children_completed
                                >= subdag.nodes.get(*parent_index).unwrap().children.len()
                            {
                                test_futures.push(harness::run_test_series(
                                    subdag.nodes.get(*parent_index).unwrap().elem,
                                    &data,
                                ))
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        rx
    }

    // sad about the amount of repetition here... perhaps we can do better once async
    // closures drop?
    fn schedule_tests_spatial(
        subdag: Dag<&'static str>,
        data: SpatialCache,
    ) -> Receiver<Result<ValidateSpatialResponse, Status>> {
        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = channel(subdag.nodes.len());
        tokio::spawn(async move {
            let mut children_completed_map: HashMap<NodeId, usize> = HashMap::new();
            let mut test_futures = FuturesUnordered::new();

            for leaf_index in subdag.leaves.clone().into_iter() {
                test_futures.push(harness::run_test_spatial(
                    subdag.nodes.get(leaf_index).unwrap().elem,
                    &data,
                ));
            }

            while let Some(res) = test_futures.next().await {
                match tx
                    .send(
                        res.clone()
                            .map_err(|e| Status::aborted(format!("a test run failed: {}", e))),
                    )
                    .await
                {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }

                match res {
                    Ok(inner) => {
                        let completed_index = subdag.index_lookup.get(inner.test.as_str()).unwrap();

                        for parent_index in
                            subdag.nodes.get(*completed_index).unwrap().parents.iter()
                        {
                            let children_completed = children_completed_map
                                .get(parent_index)
                                .map(|x| x + 1)
                                .unwrap_or(1);

                            children_completed_map.insert(*parent_index, children_completed);

                            if children_completed
                                >= subdag.nodes.get(*parent_index).unwrap().children.len()
                            {
                                test_futures.push(harness::run_test_spatial(
                                    subdag.nodes.get(*parent_index).unwrap().elem,
                                    &data,
                                ))
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        rx
    }

    // TODO: rethink string and error types here
    pub async fn validate_series_direct(
        &self,
        series_id: String,
        tests: Vec<String>,
        timerange: Timerange,
    ) -> Result<Receiver<Result<ValidateSeriesResponse, Status>>, Status> {
        if tests.is_empty() {
            return Err(Status::invalid_argument(
                "request must specify at least 1 test to be run",
            ));
        }

        let data = match self
            .data_switch
            .get_series_data(series_id.as_str(), timerange, 2)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(%e);
                return Err(Status::not_found(format!(
                    "data not found by data_switch: {}",
                    e
                )));
            }
        };

        let subdag = self
            .construct_subdag(tests)
            .map_err(|e| Status::not_found(format!("failed to construct subdag: {}", e)))?;

        Ok(Scheduler::schedule_tests_series(subdag, data))
    }

    pub async fn validate_spatial_direct(
        &self,
        spatial_id: String,
        tests: Vec<String>,
        polygon: Vec<GeoPoint>,
        time: Timestamp,
    ) -> Result<Receiver<Result<ValidateSpatialResponse, Status>>, Status> {
        if tests.is_empty() {
            return Err(Status::invalid_argument(
                "request must specify at least 1 test to be run",
            ));
        }

        let data = match self
            .data_switch
            .get_spatial_data(polygon, spatial_id.as_str(), time)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(%e);
                return Err(Status::not_found(format!(
                    "data not found by data_switch: {}",
                    e
                )));
            }
        };

        let subdag = self
            .construct_subdag(tests)
            .map_err(|e| Status::not_found(format!("failed to construct subdag: {}", e)))?;

        Ok(Scheduler::schedule_tests_spatial(subdag, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_utils::construct_fake_dag;

    #[test]
    fn test_construct_subdag() {
        let rove_service = Scheduler::new(construct_fake_dag(), DataSwitch::new(HashMap::new()));

        assert_eq!(rove_service.dag.count_edges(), 6);

        let subdag = rove_service.construct_subdag(vec!["test4"]).unwrap();

        assert_eq!(subdag.count_edges(), 1);

        let subdag = rove_service.construct_subdag(vec!["test1"]).unwrap();

        assert_eq!(subdag.count_edges(), 6);
    }
}
