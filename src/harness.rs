use crate::{
    data_switch::DataCache,
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
    FailedTest(#[from] olympian::Error),
    #[error("unknown olympian flag: {0}")]
    UnknownFlag(String),
}

pub async fn run_test_series(
    test: &str,
    cache: &DataCache,
) -> Result<ValidateSeriesResponse, Error> {
    let flags: Vec<Flag> = match test {
        // TODO: put these in a lookup table?
        "dip_check" => {
            const LEADING_PER_RUN: u8 = 2;

            // TODO: use actual test params
            // TODO: use par_iter?

            let mut result_vec = Vec::with_capacity(cache.data.len());

            // NOTE: Does data in each series have the same len?
            let series_len = cache.data[0].len();

            for i in 0..cache.data.len() {
                result_vec.push(
                    cache.data[i][(cache.num_leading_points - LEADING_PER_RUN).into()..series_len]
                        .windows((LEADING_PER_RUN + 1).into())
                        .map(|window| {
                            olympian::dip_check(window, 2., 3.)?
                                .try_into()
                                .map_err(Error::UnknownFlag)
                        })
                        .collect::<Result<Vec<Flag>, Error>>()?,
                )
            }
            result_vec[0].clone()
        }
        "step_check" => {
            const LEADING_PER_RUN: u8 = 1;

            let mut result_vec = Vec::with_capacity(cache.data.len());

            // NOTE: Does data in each series have the same len?
            let series_len = cache.data[0].len();

            for i in 0..cache.data.len() {
                result_vec.push(
                    cache.data[i][(cache.num_leading_points - LEADING_PER_RUN).into()..series_len]
                        .windows((LEADING_PER_RUN + 1).into())
                        .map(|window| {
                            olympian::step_check(window, 2., 3.)?
                                .try_into()
                                .map_err(Error::UnknownFlag)
                        })
                        .collect::<Result<Vec<Flag>, Error>>()?,
                )
            }
            result_vec[0].clone()
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
    cache: &DataCache,
) -> Result<ValidateSpatialResponse, Error> {
    let flags: Vec<Flag> = match test {
        "buddy_check" => {
            let mut result_vec = Vec::new();
            let n = cache.data.len();
            for i in 0..cache.data[0].len() {
                // TODO: change `buddy_check` to accept Option<f32>?
                let inner: Vec<f32> = cache.data.iter().map(|v| v[i].unwrap()).collect();
                result_vec.push(
                    olympian::buddy_check(
                        &cache.rtree,
                        &inner,
                        &vec![5000.; n],
                        &vec![2; n],
                        2.,
                        200.,
                        0.,
                        1.,
                        2,
                        &vec![true; n],
                    )?
                    .into_iter()
                    .map(|flag| flag.try_into().map_err(Error::UnknownFlag))
                    .collect::<Result<Vec<Flag>, Error>>()?,
                )
            }
            result_vec[0].clone()
        }
        "sct" => {
            let mut result_vec = Vec::new();
            let n = cache.data.len();

            for i in 0..cache.data[0].len() {
                // TODO: change `sct` to accept Option<f32>?
                let inner: Vec<f32> = cache.data.iter().map(|v| v[i].unwrap()).collect();
                result_vec.push(
                    olympian::sct(
                        &cache.rtree,
                        &inner,
                        5,
                        100,
                        50000.,
                        150000.,
                        5,
                        20,
                        200.,
                        10000.,
                        200.,
                        &vec![4.; n],
                        &vec![8.; n],
                        &vec![0.5; n],
                        None,
                    )?
                    .into_iter()
                    .map(|flag| flag.try_into().map_err(Error::UnknownFlag))
                    .collect::<Result<Vec<Flag>, Error>>()?,
                );
            }
            result_vec[0].clone()
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
