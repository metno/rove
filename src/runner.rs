use crate::{
    data_switch,
    util::{Flag, Timestamp},
};
use olympian::qc_tests::dip_check;
use tonic::Status;

// TODO: get rid of Status
pub async fn run_test(
    test: String,
    series_id: String,
    // TODO: convert to util::Timestamp earlier?
    time: prost_types::Timestamp,
) -> Result<Flag, Status> {
    let flag: Flag = match test.as_str() {
        "dip_check" => {
            let data = data_switch::get_series_data(
                series_id,
                data_switch::Timespec::Single(Timestamp(time.seconds)),
                2,
            )
            .await
            .map_err(|err| Status::not_found(format!("data not found by cache: {}", err)))?;
            // TODO: fix this mess...
            dip_check(
                data.0
                    .into_iter()
                    .map(|(_, v)| v)
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
