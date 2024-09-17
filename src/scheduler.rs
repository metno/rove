use crate::{
    dag::{Dag, NodeId},
    data_switch::{self, DataCache, DataSwitch, Polygon, SpaceSpec, Timerange, Timestamp},
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

/// Receiver type for QC runs
///
/// Holds information about test dependencies and data sources
#[derive(Debug, Clone)]
pub struct Scheduler<'a> {
    // TODO: separate DAGs for series and spatial tests?
    dag: Dag<&'static str>,
    data_switch: DataSwitch<'a>,
}

impl<'a> Scheduler<'a> {
    /// Instantiate a new scheduler
    pub fn new(dag: Dag<&'static str>, data_switch: DataSwitch<'a>) -> Self {
        Scheduler { dag, data_switch }
    }

    /// Construct a subdag of the given dag with only the required nodes, and their
    /// dependencies.
    fn construct_subdag(
        &self,
        required_nodes: &[impl AsRef<str>],
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

        for required in required_nodes.iter() {
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
        data: DataCache,
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
        data: DataCache,
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

    /// Run a set of timeseries QC tests on some data
    ///
    /// `series_id` is a string identifier of the data to be QCed in the form
    /// "data_source_id:data_id", where `data_source_id` is the key identifying
    /// a connector in the [`DataSwitch`](data_switch::DataSwitch), and `data_id`
    /// is an extra identifier that gets passed to the relevant DataConnector.
    /// The format of data_id is connector-specific. `timerange` represents
    /// the range of the time in the time series whose data is to be QCed.
    ///
    /// `tests` represents the QC tests to be run. Any tests these depend on
    /// will be found via the [`DAG`](Dag), and run as well.
    ///
    /// # Errors
    ///
    /// Returned from the function if:
    /// - The provided test array is empty
    /// - A test in the provided array did not have a matching entry in the DAG
    /// - The data_source_id component of the provided series_id did not have a
    ///   matching entry in the Scheduler's DataSwitch
    ///
    /// In the the returned channel if:
    /// - The test harness encounters an error on during one of the QC tests.
    ///   This will also result in the channel being closed
    pub async fn validate_series_direct(
        &self,
        series_id: impl AsRef<str>,
        tests: &[impl AsRef<str>],
        timerange: Timerange,
    ) -> Result<Receiver<Result<ValidateSeriesResponse, Error>>, Error> {
        if tests.is_empty() {
            return Err(Error::InvalidArg("must specify at least 1 test to be run"));
        }

        let (data_source_id, data_id) = series_id
            .as_ref()
            .split_once(':')
            // TODO: remove this unwrap by splitting these in the proto
            .unwrap();

        let data = match self
            .data_switch
            // TODO: num_leading and num_trailing here should be determined from the test list
            .fetch_data(
                data_source_id,
                SpaceSpec::One(data_id),
                timerange,
                2,
                2,
                // TODO: this should probably be able to be Some, needs fixing in proto
                None,
            )
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

    /// Run a set of spatial QC tests on some data
    ///
    /// `spatial_id` is a string identifier of the data to be QCed in the form
    /// "data_source_id:data_id", where `data_source_id` is the key identifying
    /// a connector in the [`DataSwitch`](data_switch::DataSwitch), and `data_id`
    /// is an extra identifier that gets passed to the relevant DataConnector.
    /// The format of data_id is connector-specific. `time` represents
    /// the timestamp of the spatial slice to be QCed, while `polygon` is a vec
    /// of lat-lon pairs that encode the vertices of a polygon defining the
    /// region of the spatial slice in which data should be QCed.
    ///
    /// `tests` represents the QC tests to be run. Any tests these depend on
    /// will be found via the [`DAG`](Dag), and run as well.
    ///
    /// # Errors
    ///
    /// Returned from the function if:
    /// - The provided test array is empty
    /// - A test in the provided array did not have a matching entry in the DAG
    /// - The data_source_id component of the provided spatial_id did not have a
    ///   matching entry in the Scheduler's DataSwitch
    ///
    /// In the the returned channel if:
    /// - The test harness encounters an error on during one of the QC tests.
    ///   This will also result in the channel being closed
    pub async fn validate_spatial_direct(
        &self,
        spatial_id: impl AsRef<str>,
        tests: &[impl AsRef<str>],
        polygon: &Polygon,
        time: Timestamp,
    ) -> Result<Receiver<Result<ValidateSpatialResponse, Error>>, Error> {
        if tests.is_empty() {
            return Err(Error::InvalidArg("must specify at least 1 test to be run"));
        }

        let (data_source_id, extra_spec) = spatial_id
            .as_ref()
            .split_once(':')
            // TODO: remove this unwrap by splitting these in the proto
            .unwrap();

        let data = match self
            .data_switch
            .fetch_data(
                data_source_id,
                SpaceSpec::Polygon(polygon),
                Timerange {
                    start: time,
                    end: time,
                },
                0,
                0,
                // TODO: This should probably be able to be None, needs fixing in proto
                Some(extra_spec),
            )
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

        let subdag = rove_service.construct_subdag(&vec!["test4"]).unwrap();

        assert_eq!(subdag.count_edges(), 1);

        let subdag = rove_service.construct_subdag(&vec!["test1"]).unwrap();

        assert_eq!(subdag.count_edges(), 6);
    }
}
