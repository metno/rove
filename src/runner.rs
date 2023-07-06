use crate::{data_switch::SeriesCache, util::Flag};
use olympian::qc_tests::dip_check;
use tonic::Status;

// TODO: get rid of Status
pub async fn run_test(
    test: String,
    data: &SeriesCache,
    // TODO: convert to util::Timestamp earlier?
) -> Result<Flag, Status> {
    let flag: Flag = match test.as_str() {
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
                return Err(Status::invalid_argument("invalid test name"));
            }
        }
    };

    Ok(flag)
}
