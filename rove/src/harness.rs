use crate::{
    data_switch::{SeriesCache, SpatialCache},
    pb::{
        Flag, GeoPoint, SeriesTestResult, SpatialTestResult, ValidateSeriesResponse,
        ValidateSpatialResponse,
    },
};
use chrono::prelude::*;
use chronoutil::DateRule;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in runner")]
    InvalidTestName(String),
    #[error("failed to run test")]
    TestFailed(#[from] olympian::Error),
    #[error("unknown olympian flag: {0}")]
    UnknownFlag(String),
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
            cache.data[(cache.num_leading_points - LEADING_PER_RUN).into()..cache.data.len()]
                .windows((LEADING_PER_RUN + 1).into())
                .map(|window| {
                    olympian::dip_check(window, 2., 3.)?
                        .try_into()
                        .map_err(Error::UnknownFlag)
                })
                .collect::<Result<Vec<Flag>, Error>>()?
        }
        "step_check" => {
            const LEADING_PER_RUN: u8 = 1;
            cache.data[(cache.num_leading_points - LEADING_PER_RUN).into()..cache.data.len()]
                .windows((LEADING_PER_RUN + 1).into())
                .map(|window| {
                    olympian::step_check(window, 2., 3.)?
                        .try_into()
                        .map_err(Error::UnknownFlag)
                })
                .collect::<Result<Vec<Flag>, Error>>()?
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
        "buddy_check" => {
            let n = cache.data.len();
            olympian::buddy_check(
                &cache.rtree,
                &cache.data,
                &vec![0.; n],
                &vec![0; n],
                0.,
                0.,
                0.,
                0.,
                0,
                &vec![true; n],
            )?
            .into_iter()
            .map(|flag| flag.try_into().map_err(Error::UnknownFlag))
            .collect::<Result<Vec<Flag>, Error>>()?
        }
        "sct" => {
            let n = cache.data.len();
            olympian::sct(
                &cache.rtree,
                &cache.data,
                0,
                0,
                0.,
                0.,
                0,
                0,
                0.,
                0.,
                0.,
                &vec![0.; n],
                &vec![0.; n],
                &vec![0.; n],
                None,
            )?
            .into_iter()
            .map(|flag| flag.try_into().map_err(Error::UnknownFlag))
            .collect::<Result<Vec<Flag>, Error>>()?
        }
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
        .rtree
        .lats
        // TODO: if lats and lons in points were in 1 vec, we could do into_iter,
        // and remove one of the zips
        .iter()
        .zip(cache.rtree.lons.iter())
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
