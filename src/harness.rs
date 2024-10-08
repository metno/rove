use crate::{
    data_switch::DataCache,
    pb::{Flag, TestResult, ValidateResponse},
    pipeline::{CheckConf, PipelineStep},
};
use chrono::prelude::*;
use chronoutil::DateRule;
use thiserror::Error;

pub const SPIKE_LEADING_PER_RUN: u8 = 1;
pub const SPIKE_TRAILING_PER_RUN: u8 = 1;
pub const STEP_LEADING_PER_RUN: u8 = 1;
pub const STEP_TRAILING_PER_RUN: u8 = 0;

#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in runner")]
    InvalidTestName(String),
    #[error("failed to run test: {0}")]
    FailedTest(#[from] olympian::Error),
    #[error("unknown olympian flag: {0}")]
    UnknownFlag(String),
}

pub fn run_test(step: &PipelineStep, cache: &DataCache) -> Result<ValidateResponse, Error> {
    let step_name = step.name.to_string();

    let flags: Vec<(String, Vec<Flag>)> = match &step.check {
        CheckConf::SpikeCheck(conf) => {
            const LEADING_PER_RUN: u8 = SPIKE_LEADING_PER_RUN;
            const TRAILING_PER_RUN: u8 = SPIKE_TRAILING_PER_RUN;

            // TODO: use par_iter?

            let mut result_vec = Vec::with_capacity(cache.data.len());

            let series_len = cache.data[0].1.len();

            for i in 0..cache.data.len() {
                result_vec.push((
                    cache.data[i].0.clone(),
                    cache.data[i].1[(cache.num_leading_points - LEADING_PER_RUN).into()
                        ..(series_len - (cache.num_trailing_points - TRAILING_PER_RUN) as usize)]
                        .windows((LEADING_PER_RUN + 1 + TRAILING_PER_RUN).into())
                        .map(|window| {
                            // TODO: the "high" param is hardcoded for now, but should be removed
                            // from olympian
                            olympian::dip_check(window, 2., conf.max)?
                                .try_into()
                                .map_err(Error::UnknownFlag)
                        })
                        .collect::<Result<Vec<Flag>, Error>>()?,
                ))
            }
            result_vec
        }
        CheckConf::StepCheck(conf) => {
            const LEADING_PER_RUN: u8 = STEP_LEADING_PER_RUN;
            const TRAILING_PER_RUN: u8 = STEP_TRAILING_PER_RUN;

            let mut result_vec = Vec::with_capacity(cache.data.len());

            // NOTE: Does data in each series have the same len?
            let series_len = cache.data[0].1.len();

            for i in 0..cache.data.len() {
                result_vec.push((
                    cache.data[i].0.clone(),
                    cache.data[i].1[(cache.num_leading_points - LEADING_PER_RUN).into()
                        ..(series_len - (cache.num_trailing_points - TRAILING_PER_RUN) as usize)]
                        .windows((LEADING_PER_RUN + 1).into())
                        .map(|window| {
                            // TODO: the "high" param is hardcoded for now, but should be removed
                            // from olympian
                            olympian::step_check(window, 2., conf.max)?
                                .try_into()
                                .map_err(Error::UnknownFlag)
                        })
                        .collect::<Result<Vec<Flag>, Error>>()?,
                ))
            }
            result_vec
        }
        CheckConf::BuddyCheck(conf) => {
            let n = cache.data.len();

            let series_len = cache.data[0].1.len();

            let mut result_vec: Vec<(String, Vec<Flag>)> = cache
                .data
                .iter()
                .map(|ts| (ts.0.clone(), Vec::with_capacity(series_len)))
                .collect();

            for i in (cache.num_leading_points as usize)
                ..(series_len - cache.num_trailing_points as usize)
            {
                // TODO: change `buddy_check` to accept Option<f32>?
                let inner: Vec<f32> = cache.data.iter().map(|v| v.1[i].unwrap()).collect();

                let spatial_result = olympian::buddy_check(
                    &cache.rtree,
                    &inner,
                    &conf.radii,         // &vec![5000.; n],
                    &conf.nums_min,      // &vec![2; n],
                    conf.threshold,      // 2.,
                    conf.max_elev_diff,  // 200.,
                    conf.elev_gradient,  // 0.,
                    conf.min_std,        // 1.,
                    conf.num_iterations, // 2,
                    // TODO: should we be setting this dynamically? from where?
                    &vec![true; n],
                )?;

                for (i, flag) in spatial_result.into_iter().map(Flag::try_from).enumerate() {
                    result_vec[i].1.push(flag.map_err(Error::UnknownFlag)?);
                }
            }
            result_vec
        }
        CheckConf::Sct(conf) => {
            // TODO: evaluate whether we will need this to extend param vectors from conf
            // if the checks accept single values (which they should) then we don't need this.
            // anyway I think if we have dynamic values for these we can match them to the data
            // when fetching them.
            let n = cache.data.len();

            let series_len = cache.data[0].1.len();

            let mut result_vec: Vec<(String, Vec<Flag>)> = cache
                .data
                .iter()
                .map(|ts| (ts.0.clone(), Vec::with_capacity(series_len)))
                .collect();

            for i in (cache.num_leading_points as usize)
                ..(series_len - cache.num_trailing_points as usize)
            {
                // TODO: change `sct` to accept Option<f32>?
                let inner: Vec<f32> = cache.data.iter().map(|v| v.1[i].unwrap()).collect();
                // TODO: make it so olympian can accept the conf as one param?
                let spatial_result = olympian::sct(
                    &cache.rtree,
                    &inner,
                    conf.num_min,              // 5,
                    conf.num_max,              // 100,
                    conf.inner_radius,         // 50000.,
                    conf.outer_radius,         // 150000.,
                    conf.num_iterations,       // 5,
                    conf.num_min_prof,         // 20,
                    conf.min_elev_diff,        // 200.,
                    conf.min_horizontal_scale, // 10000.,
                    conf.vertical_scale,       // 200.,
                    // TODO: we shouldn't need to extend these vectors, it should be handled
                    // better in olympian
                    &vec![conf.pos[0]; n],  // &vec![4.; n],
                    &vec![conf.neg[0]; n],  // &vec![8.; n],
                    &vec![conf.eps2[0]; n], // &vec![0.5; n],
                    None,
                )?;

                for (i, flag) in spatial_result.into_iter().map(Flag::try_from).enumerate() {
                    result_vec[i].1.push(flag.map_err(Error::UnknownFlag)?);
                }
            }
            result_vec
        }
        _ => {
            // used for integration testing
            if step_name.starts_with("test") {
                vec![("test".to_string(), vec![Flag::Inconclusive])]
            } else {
                return Err(Error::InvalidTestName(step_name));
            }
        }
    };

    let date_rule = DateRule::new(
        // TODO: make sure this start time is actually correct
        Utc.timestamp_opt(cache.start_time.0, 0).unwrap(),
        cache.period,
    );
    let results = flags
        .into_iter()
        .flat_map(|flag_series| {
            flag_series
                .1
                .into_iter()
                .zip(date_rule)
                .zip(std::iter::repeat(flag_series.0))
        })
        .map(|((flag, time), identifier)| TestResult {
            time: Some(prost_types::Timestamp {
                seconds: time.timestamp(),
                nanos: 0,
            }),
            identifier,
            flag: flag.into(),
        })
        .collect();

    Ok(ValidateResponse {
        test: step_name,
        results,
    })
}
