use crate::{Error, FrostObs};
use chrono::prelude::*;
use rstar::RTree;
use olympian::points::{CoordinateType, Points};
use rove::data_switch::{SpatialCache, Timestamp};
use rove::pb::util::GeoPoint;

// have to repeat from series? 
// or is it possible to import - function `crate::series::extract_obs` exists but is inaccessible
fn extract_obs(mut resp: serde_json::Value) -> Result<Vec<FrostObs>, Error> {
    let obs_portion = resp
        .get_mut("data")
        .ok_or(Error::FindObs(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(Error::FindObs(
            "couldn't find tseries field on data".to_string(),
        ))?
        .get_mut(0)
        .ok_or(Error::FindObs("tseries array is empty".to_string()))?
        .get_mut("observations")
        .ok_or(Error::FindObs(
            "couldn't observations data field on 1st tseries".to_string(),
        ))?
        .take();

    let obs: Vec<FrostObs> = serde_json::from_value(obs_portion)?;

    Ok(obs)
}

fn json_to_spatial_cache(
    resp: serde_json::Value,
    polygon: Vec<GeoPoint>,
    extra_spec: &str,
    timestamp: DateTime<Utc>,
) -> Result<SpatialCache, Error> {
    let obses: Vec<FrostObs> = extract_obs(resp)?;

    let mut data = Vec::new();
    let mut tree = RTree::new();
    let mut rtree: Points = Points{ 
        tree: tree,
        lats: Vec::new(),
        lons: Vec::new(),
        elevs: Vec::new(),
        lafs: Vec::new(),
        ctype: CoordinateType::Cartesian,
     };

    Ok(SpatialCache {
        rtree,
        data,
    })
}

pub async fn get_spatial_data_inner(
    polygon: Vec<GeoPoint>,
    extra_spec: &str,
    timestamp: Timestamp,
) -> Result<SpatialCache, Error> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let elementids: String = (&extra_spec).to_string();
    let time = Utc.timestamp_opt(timestamp.0, 0).unwrap();
    
    // TODO: parse the vector of geopoints into an appropriate string
    let mut s = String::new();
    s.push_str("{");
    for coord in polygon.iter() {
        s.push_str(&coord.to_string())
    }
    s.push_str("}");

    let resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("polygon", s),
            ("elementids", elementids),
            ("incobs", "true".to_string()),
            (
                "time",
                format!(
                    "{}/{}",
                    (time).to_rfc3339_opts(SecondsFormat::Secs, true),
                    (time).to_rfc3339_opts(SecondsFormat::Secs, true)
                ),
            ),
        ])
        .send()
        .await?
        .json()
        .await?;

    // TODO: send this part to rayon?
    json_to_spatial_cache(
        resp,
        polygon,
        extra_spec,
        time,
    )
}


#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"
{
}"#;

    #[test]
    fn test_json_to_spatial_cache() {
        let resp = serde_json::from_str(RESP).unwrap();

        let polygon:Vec<GeoPoint> = vec![GeoPoint{lat: 59.88, lon: 10.64},GeoPoint{lat: 60.00, lon: 10.56},GeoPoint{lat: 59.99, lon: 10.88}];
        let element: &str = r#"air_temperature"#;

        println!("{}", resp);

        let spatial_cache = json_to_spatial_cache(
            resp,
            polygon,
            element,
            Utc.with_ymd_and_hms(2023, 6, 30, 12, 0, 0).unwrap(),
        )
        .unwrap();
    }
}