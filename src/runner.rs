use crate::{data_switch, util::Flag};
use olympian::qc_tests::dip_check;
use prost_types::Timestamp;
use tonic::Status;

// TODO: get rid of Status
pub async fn run_test(test: String, series_id: String, time: Timestamp) -> Result<Flag, Status> {
    let flag: Flag = match test.as_str() {
        "dip_check" => {
            let data = data_switch::get_timeseries_data(
                series_id,
                data_switch::Timespec::Single(time.seconds),
                2,
            )
            .await
            .map_err(|err| Status::not_found(format!("data not found by cache: {}", err)))?;
            dip_check(data, 2., 3.).into() //TODO use actual test params
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
