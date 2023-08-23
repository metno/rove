use crate::{Error, FrostLatLonElev, FrostLocation, FrostObs};
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

    // get the value of the observations
    let obs: Vec<FrostObs> = ts_portion
        .iter_mut()
        .map(|i| i.get_mut("observations").unwrap().take())
        .map(|mut i| serde_json::from_value(i.get_mut(0).unwrap().take()).unwrap())
        .collect();

    Ok(obs)
}

fn extract_metadata(
    mut resp: serde_json::Value,
    time: DateTime<Utc>,
) -> Result<Vec<FrostLatLonElev>, Error> {
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

    // metadata
    let extra_portion = ts_portion
        .iter_mut()
        .map(|i| i.get_mut("header").unwrap().take())
        .map(|mut i| i.get_mut("extra").unwrap().take());

    // get the lat/lon, elev
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
        for l in loc {
            if time.cmp(&l.from) == Ordering::Greater && time.cmp(&l.to) == Ordering::Less {
                latlonelev.push(l.value)
            }
        }
    }

    Ok(latlonelev)
}

fn json_to_spatial_cache(
    resp: serde_json::Value,
    _polygon: Vec<GeoPoint>,
    _element: &str,
    timestamp: DateTime<Utc>,
) -> Result<SpatialCache, Error> {
    let obses: Vec<FrostObs> = extract_obs(resp.clone())?;
    let latloneleves: Vec<FrostLatLonElev> = extract_metadata(resp, timestamp)?;
    assert_eq!(obses.len(), latloneleves.len());

    // todo: make mutable and do something to these..
    let mut lats = Vec::new();
    let mut lons = Vec::new();
    let mut elevs = Vec::new();
    let mut values = Vec::new();

    for o in obses {
        values.push(o.body.value);
    }
    for lle in latloneleves {
        lats.push(lle.latitude);
        lons.push(lle.longitude);
        elevs.push(lle.elevation);
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

    // Parse the vector of geopoints into an appropriate string
    // TODO: Move into seperate function?
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

    //println!("{}", s);
    //println!("{}", elementids);
    //println!("{}", time);

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
        .await?; //println!("{}", resp);

    // TODO: send this part to rayon?
    json_to_spatial_cache(resp, polygon, element, time)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"
{
    "data": {
        "tstype": "met.no/filter",
        "tseries": [
        {
        "header": {
        "id": {
        "level": 0,
        "parameterid": 211,
        "sensor": 0,
        "stationid": 18700
        },
        "extra": {
        "element": {},
        "station": {
        "location": [
        {
        "from": "1931-01-01T00:00:00Z",
        "to": "1940-12-31T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "85",
        "latitude": "59.939200",
        "longitude": "10.718600"
        }
        },
        {
        "from": "1941-01-01T00:00:00Z",
        "to": "9999-01-01T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "94",
        "latitude": "59.942300",
        "longitude": "10.720000"
        }
        }
        ],
        "shortname": "Oslo (Blindern)"
        },
        "timeseries": {}
        },
        "available": {
        "from": "1937-01-01T06:00:00Z"
        }
        },
        "observations": [
        {
        "time": "2023-08-13T18:00:00Z",
        "body": {
        "qualitycode": "0",
        "value": "17"
        }
        }
        ]
        },
        {
        "header": {
        "id": {
        "level": 0,
        "parameterid": 211,
        "sensor": 0,
        "stationid": 18315
        },
        "extra": {
        "element": {
        "description": "Air temperature (default 2 m above ground), present value",
        "id": "air_temperature",
        "name": "Air temperature",
        "unit": "degC"
        },
        "station": {
        "location": [
        {
        "from": "2016-01-07T00:00:00Z",
        "to": "9999-01-01T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "37",
        "latitude": "59.919000",
        "longitude": "10.762300"
        }
        }
        ],
        "shortname": "Sofienberg "
        },
        "timeseries": {
        "geometry": {
        "level": {
        "unit": "m",
        "value": "2"
        }
        },
        "quality": {
        "exposure": [
        {
        "from": "2016-01-07T00:00:00Z",
        "to": "2016-01-07T00:00:00Z",
        "value": "5"
        },
        {
        "from": "2015-07-07T22:00:00Z",
        "to": "2015-07-07T22:00:00Z",
        "value": "unknown"
        }
        ],
        "performance": [
        {
        "from": "2016-01-07T00:00:00Z",
        "to": "2016-01-07T00:00:00Z",
        "value": "E"
        },
        {
        "from": "2015-07-07T22:00:00Z",
        "to": "2015-07-07T22:00:00Z",
        "value": "unknown"
        }
        ]
        },
        "timeoffset": "PT0H",
        "timeresolution": "PT1M"
        }
        },
        "available": {
        "from": "2015-07-07T22:00:00Z"
        }
        },
        "observations": [
        {
        "time": "2023-08-13T18:00:00Z",
        "body": {
        "qualitycode": "0",
        "value": "18.1000004"
        }
        }
        ]
        },
        {
        "header": {
        "id": {
        "level": 0,
        "parameterid": 211,
        "sensor": 0,
        "stationid": 18950
        },
        "extra": {
        "element": {
        "description": "Air temperature (default 2 m above ground), present value",
        "id": "air_temperature",
        "name": "Air temperature",
        "unit": "degC"
        },
        "station": {
        "location": [
        {
        "from": "1927-08-01T00:00:00Z",
        "to": "1975-12-01T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "514",
        "latitude": "59.984700",
        "longitude": "10.669300"
        }
        },
        {
        "from": "1975-12-01T00:00:00Z",
        "to": "1976-12-31T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "514",
        "latitude": "59.984700",
        "longitude": "10.669300"
        }
        },
        {
        "from": "1997-09-16T00:00:00Z",
        "to": "2012-01-04T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "514",
        "latitude": "59.984700",
        "longitude": "10.669300"
        }
        },
        {
        "from": "2012-01-04T00:00:00Z",
        "to": "9999-01-01T00:00:00Z",
        "value": {
        "elevation(masl/hs)": "514",
        "latitude": "59.984700",
        "longitude": "10.669300"
        }
        }
        ],
        "shortname": "Tryvannshøgda"
        },
        "timeseries": {
        "geometry": {
        "level": {
        "unit": "m",
        "value": "2"
        }
        },
        "quality": {
        "exposure": [
        {
        "from": "1943-01-01T06:00:00Z",
        "to": "1943-01-01T06:00:00Z",
        "value": "1"
        },
        {
        "from": "1975-12-31T18:00:00Z",
        "to": "1975-12-31T18:00:00Z",
        "value": "unknown"
        },
        {
        "from": "1997-09-16T00:00:00Z",
        "to": "1997-09-16T00:00:00Z",
        "value": "1"
        }
        ],
        "performance": [
        {
        "from": "1943-01-01T06:00:00Z",
        "to": "1943-01-01T06:00:00Z",
        "value": "unknown"
        }
        ]
        },
        "timeoffset": "PT0H",
        "timeresolution": "PT1H"
        }
        },
        "available": {
        "from": "1943-01-01T06:00:00Z"
        }
        },
        "observations": [
        {
        "time": "2023-08-13T18:00:00Z",
        "body": {
        "qualitycode": "0",
        "value": "13.3000002"
        }
        }
        ]
        }
        ]
        }
}"#;

    #[test]
    fn test_json_to_spatial_cache() {
        let resp = serde_json::from_str(RESP).unwrap();

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
        let element: &str = r#"frost:air_temperature"#;

        let spatial_cache = json_to_spatial_cache(
            resp,
            polygon,
            element,
            Utc.with_ymd_and_hms(2023, 6, 30, 12, 0, 0).unwrap(),
        )
        .unwrap();

        assert_eq!(spatial_cache.data.len(), 3);
    }

    // Still need this test? was used during testing...
    /*
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
    */
}
