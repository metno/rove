use olympian::points::Points;

use serde::{de::Error, Deserialize, Deserializer};

#[derive(Deserialize, Debug)]
struct FrostObsBody {
    // #[serde(deserialize_with = "des_value")]
    value: String,
}

#[derive(Deserialize, Debug)]
struct FrostObs {
    body: FrostObsBody,
    time: String,
}

fn des_value<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    // u64::from_str_radix(&s[2..], 16).map_err(D::Error::custom)
    // deserializer.deserialize_str(s)
    String::from(s).parse().map_err(D::Error::custom)
}
pub async fn get_timeseries_data(
    _station_id: u32,
    _unix_timestamp: i64,
) -> Result<[f32; 3], Box<dyn std::error::Error>> {
    let resp: serde_json::Value = reqwest::get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get?elementids=air_temperature&stationids=18700&incobs=true&time=latest").await?.json().await?;

    let obs: Vec<FrostObs> =
        serde_json::from_value(resp["data"]["tseries"][0]["observations"].to_owned()).unwrap();

    println!(
        "{:?}",
        obs.into_iter()
            .map(|obs| obs.body.value)
            .collect::<Vec<String>>()
    );

    Ok([1., 1., 1.]) // TODO get actual data
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
