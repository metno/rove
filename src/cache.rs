use std::collections::HashMap;
use titanlib_rs::points::Points;

pub async fn get_timeseries_data(
    _station_id: u32,
    _unix_timestamp: i64,
) -> Result<[f32; 3], Box<dyn std::error::Error>> {
    let _resp = reqwest::get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get?elementids=air_temperature&stationids=18700&incobs=true&time=latest").await?.json::<HashMap<String, String>>().await?;

    return Ok([1., 1., 1.]); // TODO get actual data
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
