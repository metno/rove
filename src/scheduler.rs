use crate::{
    data_switch::{self, DataCache, DataSwitch, SpaceSpec, TimeSpec},
    harness,
    // TODO: rethink this dependency?
    pb::ValidateResponse,
    pipeline::Pipeline,
};
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};

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
/// Holds information about test pipelines and data sources
#[derive(Debug, Clone)]
pub struct Scheduler<'a> {
    // this is pub so that the server can determine the number of checks in a pipeline to size
    // its channel with. can be made private if the server functionality is deprecated
    #[allow(missing_docs)]
    pub pipelines: HashMap<String, Pipeline>,
    data_switch: DataSwitch<'a>,
}

impl<'a> Scheduler<'a> {
    /// Instantiate a new scheduler
    pub fn new(pipelines: HashMap<String, Pipeline>, data_switch: DataSwitch<'a>) -> Self {
        Scheduler {
            pipelines,
            data_switch,
        }
    }

    fn schedule_tests(
        pipeline: Pipeline,
        data: DataCache,
    ) -> Receiver<Result<ValidateResponse, Error>> {
        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        // TODO: Should we keep this channel or just return everything together?
        // the original idea behind the channel was that it was best to return flags ASAP, and the
        // channel allowed us to do that without waiting for later tests to finish. Now I'm not so
        // convinced of its utility. Since we won't run the combi check to generate end user flags
        // until the full pipeline is finished, it doesn't seem like the individual flags have any
        // use before that point.
        let (tx, rx) = channel(pipeline.steps.len());
        tokio::spawn(async move {
            for step in pipeline.steps.iter() {
                let result = harness::run_test(step, &data);

                match tx.send(result.map_err(Error::Runner)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
        });

        rx
    }

    /// Run a set of QC tests on some data
    ///
    /// `data_source` is the key identifying a connector in the
    /// [`DataSwitch`](data_switch::DataSwitch).
    /// `backing_sources` a list of keys similar to `data_source`, but data
    /// from these will only be used to QC data from `data_source` and will not
    /// themselves be QCed.
    /// `time_spec` and `space_spec` narrow down what data to QC, more info
    /// on what these mean and how to construct them can be found on their
    /// own doc pages.
    /// `extra_spec` is an extra identifier that gets passed to the relevant
    /// DataConnector. The format of `extra_spec` is connector-specific.
    ///
    /// `tests` represents the QC tests to be run. Any tests these depend on
    /// will be found via the [`DAG`](Dag), and run as well.
    ///
    /// # Errors
    ///
    /// Returned from the function if:
    /// - The provided test array is empty
    /// - A test in the provided array did not have a matching entry in the DAG
    /// - The data_source string did not have a matching entry in the
    ///   Scheduler's DataSwitch
    ///
    /// In the the returned channel if:
    /// - The test harness encounters an error on during one of the QC tests.
    ///   This will also result in the channel being closed
    pub async fn validate_direct(
        &self,
        data_source: impl AsRef<str>,
        // TODO: we should actually use these
        _backing_sources: &[impl AsRef<str>],
        time_spec: &TimeSpec,
        space_spec: &SpaceSpec,
        // TODO: should we allow specifying multiple pipelines per call?
        test_pipeline: impl AsRef<str>,
        extra_spec: Option<&str>,
    ) -> Result<Receiver<Result<ValidateResponse, Error>>, Error> {
        let pipeline = self
            .pipelines
            .get(test_pipeline.as_ref())
            .ok_or(Error::InvalidArg("must specify at least 1 test to be run"))?;

        let data = match self
            .data_switch
            .fetch_data(
                data_source.as_ref(),
                space_spec,
                time_spec,
                // TODO: derive num_leading and num_trailing from pipeline
                1,
                1,
                extra_spec,
            )
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(%e);
                return Err(Error::DataSwitch(e));
            }
        };

        // TODO: can probably get rid of this clone if we get rid of the channels in
        // schedule_tests
        Ok(Scheduler::schedule_tests(pipeline.clone(), data))
    }
}
