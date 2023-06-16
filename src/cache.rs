use olympian::points::Points;

use serde::{de::Error, Deserialize, Deserializer};

#[derive(Deserialize, Debug)]
struct FrostObsBody {
    #[serde(deserialize_with = "des_value")]
    value: f32,
}

#[derive(Deserialize, Debug)]
struct FrostObs {
    body: FrostObsBody,
    _time: String,
}

fn des_value<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(D::Error::custom)
}

pub async fn get_timeseries_data(
    _series_id: String,
    _unix_timestamp: i64,
) -> Result<[f32; 3], Box<dyn std::error::Error>> {
    let mut resp: serde_json::Value = reqwest::get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get?elementids=air_temperature&stationids=18700&incobs=true&time=latest").await?.json().await?;

    let obs_portion = resp
        .get_mut("data")
        .unwrap()
        .get_mut("tseries")
        .unwrap()
        .get_mut(0)
        .unwrap()
        .get_mut("observations")
        .unwrap()
        .take();

    let obs: Vec<FrostObs> = serde_json::from_value(obs_portion)?;

    println!(
        "{:?}",
        obs.into_iter()
            .map(|obs| obs.body.value)
            .collect::<Vec<f32>>()
    );

    Ok([1., 1., 1.]) // TODO get actual data
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
