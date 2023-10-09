use crate::{
    dag::{Dag, NodeId},
    data_switch::{self, DataSwitch, GeoPoint, SeriesCache, SpatialCache, Timerange, Timestamp},
    harness,
    // TODO: rethink this dependency?
    pb::{ValidateSeriesResponse, ValidateSpatialResponse},
};
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_stream::StreamExt;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in dag")]
    TestNotInDag(String),
    #[error("failed to run test: {0}")]
    Runner(#[from] harness::Error),
    #[error("invalid argument: {0}")]
    InvalidArg(&'static str),
    #[error("data switch failed to find data: {0}")]
    DataSwitch(#[from] data_switch::Error),
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
                    let new_index = subdag.add_node(dag.nodes.get(*child_index).unwrap().elem);
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
                let subdag_index = subdag.add_node(self.dag.nodes.get(*index).unwrap().elem);

                nodes_visited.insert(*index, subdag_index);

                add_descendants(&self.dag, &mut subdag, *index, &mut nodes_visited);
            }
        }

        Ok(subdag)
    }

    fn schedule_tests_series(
        subdag: Dag<&'static str>,
        data: SeriesCache,
    ) -> Receiver<Result<ValidateSeriesResponse, Error>> {
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
                match tx.send(res.clone().map_err(Error::Runner)).await {
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
    ) -> Receiver<Result<ValidateSpatialResponse, Error>> {
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
                match tx.send(res.clone().map_err(Error::Runner)).await {
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

    pub async fn validate_series_direct<T: AsRef<str>>(
        &self,
        series_id: T,
        tests: Vec<T>,
        timerange: Timerange,
    ) -> Result<Receiver<Result<ValidateSeriesResponse, Error>>, Error> {
        if tests.is_empty() {
            return Err(Error::InvalidArg("must specify at least 1 test to be run"));
        }

        let data = match self
            .data_switch
            .get_series_data(series_id.as_ref(), timerange, 2)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(%e);
                return Err(Error::DataSwitch(e));
            }
        };

        let subdag = self.construct_subdag(tests)?;

        Ok(Scheduler::schedule_tests_series(subdag, data))
    }

    pub async fn validate_spatial_direct<T: AsRef<str>>(
        &self,
        spatial_id: T,
        tests: Vec<T>,
        polygon: Vec<GeoPoint>,
        time: Timestamp,
    ) -> Result<Receiver<Result<ValidateSpatialResponse, Error>>, Error> {
        if tests.is_empty() {
            return Err(Error::InvalidArg("must specify at least 1 test to be run"));
        }

        let data = match self
            .data_switch
            .get_spatial_data(polygon, spatial_id.as_ref(), time)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(%e);
                return Err(Error::DataSwitch(e));
            }
        };

        let subdag = self.construct_subdag(tests)?;

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
