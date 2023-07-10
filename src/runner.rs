use crate::{
    data_switch::SeriesCache,
    util::{
        pb::coordinator::{SeriesTestResult, ValidateSeriesResponse},
        pb::util::Flag,
    },
};
use chrono::prelude::*;
use chronoutil::DateRule;
use olympian::qc_tests::dip_check;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in runner")]
    InvalidTestName(String),
}

pub async fn run_test(test: &str, cache: &SeriesCache) -> Result<ValidateSeriesResponse, Error> {
    let flags: Vec<Flag> = match test {
        "dip_check" => {
            // TODO: remove unnecessary preceeding vals?
            // TODO: use actual test params
            // TODO: use par_iter?
            // TODO: do something about that unwrap?
            cache
                .data
                .windows(3)
                .map(|window| dip_check(window, 2., 3.).unwrap().into())
                .collect()
        }
        _ => {
            if test.starts_with("test") {
                vec![Flag::Inconclusive]
            } else {
                return Err(Error::InvalidTestName(test.to_string()));
            }
        }
    };

    let results = DateRule::new(
        // TODO: make sure this start time is actually correct
        Utc.timestamp_opt(cache.start_time.0, 0).unwrap(),
        cache.period,
    )
    .zip(flags.into_iter())
    .map(|(time, flag)| SeriesTestResult {
        time: Some(prost_types::Timestamp {
            seconds: time.timestamp(),
            nanos: 0,
        }),
        flag: flag.into(),
    })
    .collect();

    Ok(ValidateSeriesResponse {
        test: test.to_string(),
        results,
    })
}
