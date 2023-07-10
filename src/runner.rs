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

// TODO: remove when dip takes the right args
fn dip_wrapper(data: &[Option<f32>], high: f32, max: f32) -> olympian::qc_tests::Flag {
    dip_check(
        data.iter()
            .map(|opt| opt.unwrap_or(0.))
            .collect::<Vec<f32>>()
            .try_into()
            .unwrap(),
        high,
        max,
    )
}

pub async fn run_test(test: &str, cache: &SeriesCache) -> Result<ValidateSeriesResponse, Error> {
    let flags: Vec<Flag> = match test {
        "dip_check" => {
            // TODO: remove unnecessary preceeding vals?
            // TODO: use actual test params
            // TODO: use par_iter?
            cache
                .data
                .windows(3)
                .map(|window| dip_wrapper(window, 2., 3.).into())
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
