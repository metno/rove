use crate::{
    data_switch::{SeriesCache, SpatialCache},
    util::pb::{
        coordinator::{
            SeriesTestResult, SpatialTestResult, ValidateSeriesResponse, ValidateSpatialResponse,
        },
        util::{Flag, GeoPoint},
    },
};
use chrono::prelude::*;
use chronoutil::DateRule;
use olympian::qc_tests::dip_check;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in runner")]
    InvalidTestName(String),
}

pub async fn run_test_series(
    test: &str,
    cache: &SeriesCache,
) -> Result<ValidateSeriesResponse, Error> {
    let flags: Vec<Flag> = match test {
        // TODO: put these in a lookup table?
        "dip_check" => {
            const LEADING_PER_RUN: u8 = 2;

            // TODO: use actual test params
            // TODO: use par_iter?
            // TODO: do something about that unwrap?
            cache.data[(cache.num_leading_points - LEADING_PER_RUN).into()..cache.data.len()]
                .windows((LEADING_PER_RUN + 1).into())
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
    .skip(cache.num_leading_points.into())
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

#[allow(clippy::match_single_binding)]
pub async fn run_test_spatial(
    test: &str,
    cache: &SpatialCache,
) -> Result<ValidateSpatialResponse, Error> {
    let flags: Vec<Flag> = match test {
        _ => {
            if test.starts_with("test") {
                vec![Flag::Inconclusive]
            } else {
                // TODO: have more specific error for spatial vs series here?
                return Err(Error::InvalidTestName(test.to_string()));
            }
        }
    };

    let results = cache
        .data
        .lats
        // TODO: if lats and lons in points were in 1 vec, we could do into_iter,
        // and remove one of the zips
        .iter()
        .zip(cache.data.lons.iter())
        .map(|(lat, lon)| GeoPoint {
            lat: *lat,
            lon: *lon,
        })
        .zip(flags.into_iter())
        .map(|(location, flag)| SpatialTestResult {
            // TODO: get to the bottom of exactly why the Some is needed
            location: Some(location),
            flag: flag.into(),
        })
        .collect();

    Ok(ValidateSpatialResponse {
        test: test.to_string(),
        results,
    })
}
