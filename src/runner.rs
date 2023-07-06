use crate::{data_switch::SeriesCache, util::Flag};
use olympian::qc_tests::dip_check;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("test name {0} not found in runner")]
    InvalidTestName(String),
}

pub async fn run_test(test: &str, data: &SeriesCache) -> Result<(String, Flag), Error> {
    let flag: Flag = match test {
        "dip_check" => {
            // TODO: fix this mess... copying, unwrapping
            dip_check(
                data.0
                    .iter()
                    .map(|(_, v)| *v)
                    .collect::<Vec<f32>>()
                    .try_into()
                    .unwrap(),
                2.,
                3.,
            )
            .into() //TODO use actual test params
        }
        _ => {
            if test.starts_with("test") {
                Flag::Inconclusive
            } else {
                return Err(Error::InvalidTestName(test.to_string()));
            }
        }
    };

    Ok((test.to_string(), flag))
}
