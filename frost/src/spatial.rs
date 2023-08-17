use crate::{Error, FrostLocation, FrostObs, FrostLatLonElev};
use chrono::prelude::*;
use core::cmp::Ordering;
use rove::data_switch::{SpatialCache, Timestamp};
use rove::pb::util::GeoPoint;

fn extract_obs(mut resp: serde_json::Value) -> Result<Vec<FrostObs>, Error> {
    let ts_portion: &mut Vec<serde_json::Value> = resp
        .get_mut("data")
        .ok_or(Error::FindObs(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(Error::FindObs(
            "couldn't find tseries field on data".to_string(),
        ))?
        .as_array_mut()
        .take()
        .unwrap();

    println!("len {}", ts_portion.len());

    // get the value of the observations
    let obs: Vec<FrostObs> = ts_portion
        .iter_mut()
        .map(|i| i.get_mut("observations").unwrap().take())
        .map(|mut i| serde_json::from_value(i.get_mut(0).unwrap().take()).unwrap())
        .collect();

    // metadata
    let extra_portion = ts_portion
        .iter_mut()
        .map(|i| i.get_mut("header").unwrap().take())
        .map(|mut i| i.get_mut("extra").unwrap().take());

    // get the lat/lon, elev
    let mut i = 0;
    let mut latlonelev: Vec<FrostLatLonElev> = vec![];
    for mut extra in extra_portion {
        let location = extra
            .get_mut("station")
            .unwrap()
            .take()
            .get_mut("location")
            .unwrap()
            .take();

        let loc: Vec<FrostLocation> = serde_json::from_value(location).unwrap();

        // find the one that is for the timestamp we have in the data
        let i_time = obs[i].time;
        for l in loc {
            if i_time.cmp(&l.from) == Ordering::Greater && i_time.cmp(&l.to) == Ordering::Less {
                println!("location {:?}", l);
                latlonelev.push(l.value)
            }
        }
        i = i + 1;
    }
    assert_eq!(obs.len(), latlonelev.len());

    Ok(obs)
}

fn json_to_spatial_cache(
    resp: serde_json::Value,
    _polygon: Vec<GeoPoint>,
    _element: &str,
    _timestamp: DateTime<Utc>,
) -> Result<SpatialCache, Error> {
    let obses: Vec<FrostObs> = extract_obs(resp)?;

    // todo: make mutable and do something to these..
    let lats = Vec::new();
    let lons = Vec::new();
    let elevs = Vec::new();
    let mut values = Vec::new();

    for o in obses {
        values.push(o.body.value)
    }

    Ok(SpatialCache::new(lats, lons, elevs, values))
}

pub async fn get_spatial_data_inner(
    polygon: Vec<GeoPoint>,
    element: &str,
    timestamp: Timestamp,
) -> Result<SpatialCache, Error> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let elementids: String = (&element).to_string();
    let time = Utc.timestamp_opt(timestamp.0, 0).unwrap();

    // TODO: parse the vector of geopoints into an appropriate string
    // Move into seperate function?
    let mut s = String::new();
    s.push('[');
    let mut first = true;
    for coord in polygon.iter() {
        if !first {
            s.push(',');
        }
        s.push('{');
        s.push_str((format!("\"lat\":{},\"lon\":{}", coord.lat, coord.lon)).as_str());
        s.push('}');
        first = false;
    }
    s.push(']');

    println!("{}", s);
    println!("{}", elementids);
    println!("{}", time);

    let resp: serde_json::Value = client
        .get("https://v1.frost-dev.k8s.met.no//api/v1/obs/met.no/filter/get")
        .query(&[
            ("polygon", s),
            ("elementids", elementids),
            ("incobs", "true".to_string()),
            (
                "time",
                format!("{}", (time).to_rfc3339_opts(SecondsFormat::Secs, true),),
            ),
            ("geopostype", "stationary".to_string()),
        ])
        .send()
        .await?
        .json()
        .await?;

    //println!("{}", resp);

    // TODO: send this part to rayon?
    json_to_spatial_cache(resp, polygon, element, time)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"
{
}"#;

    /*
    #[test]
    fn test_json_to_spatial_cache() {
        let resp = serde_json::from_str(RESP).unwrap();

        let polygon:Vec<GeoPoint> = vec![GeoPoint{lat: 59.88, lon: 10.64},GeoPoint{lat: 60.00, lon: 10.56},GeoPoint{lat: 59.99, lon: 10.88}];
        let extra_spec: &str = r#"frost:air_temperature"#;

        let spatial_cache = json_to_spatial_cache(
            resp,
            polygon,
            element,
            Utc.with_ymd_and_hms(2023, 6, 30, 12, 0, 0).unwrap(),
        )
        .unwrap();
    }
    */
    #[tokio::test]
    async fn test_get_spatial_data_inner() {
        let polygon: Vec<GeoPoint> = vec![
            GeoPoint {
                lat: 59.88,
                lon: 10.57,
            },
            GeoPoint {
                lat: 60.00,
                lon: 10.58,
            },
            GeoPoint {
                lat: 59.99,
                lon: 10.83,
            },
            GeoPoint {
                lat: 59.88,
                lon: 10.87,
            },
        ];
        let element: &str = r#"air_temperature"#;
        let time = Timestamp::from(rove::data_switch::Timestamp(1691949600));

        let frost_resp = get_spatial_data_inner(polygon, element, time)
            .await
            .unwrap();
        println!("{:?}", frost_resp.data);
    }
}
