use crate::frost::{Error, FrostLatLonElev, FrostLocation, FrostObs};
use chrono::prelude::*;
use rove::{
    data_switch::{self, SpatialCache, Timestamp},
    pb::GeoPoint,
};

fn extract_metadata(
    mut header: serde_json::Value,
    time: DateTime<Utc>,
) -> Result<FrostLatLonElev, Error> {
    let location = header
        .get_mut("extra")
        .ok_or(Error::FindLocation(
            "couldn't find extra in header".to_string(),
        ))?
        .get_mut("station")
        .ok_or(Error::FindLocation(
            "couldn't find station field in extra".to_string(),
        ))?
        .get_mut("location")
        .ok_or(Error::FindLocation(
            "couldn't find location field in station".to_string(),
        ))?
        .take();

    let loc = serde_json::from_value::<Vec<FrostLocation>>(location)?;

    let lat_lon_elev = loc
        .into_iter()
        .find(|l| time > l.from && time < l.to)
        .ok_or(Error::FindLocation(
            "couldn't find relevant location for this observation".to_string(),
        ))?
        .value;

    Ok(lat_lon_elev)
}

fn extract_data(
    mut resp: serde_json::Value,
    time: DateTime<Utc>,
) -> Result<Vec<(FrostObs, FrostLatLonElev)>, Error> {
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
        .ok_or(Error::FindObs("couldn't get array of tseries".to_string()))?;

    let data = ts_portion
        .iter_mut()
        .map(|ts| {
            let obs: FrostObs = serde_json::from_value(
                ts.get_mut("observations")
                    .ok_or(Error::FindObs(
                        "couldn't find observations field on tseries".to_string(),
                    ))?
                    .get_mut(0)
                    .ok_or(Error::FindObs("couldn't find first obs".to_string()))?
                    .take(),
            )?;
            let lat_lon_elev = extract_metadata(
                ts.get_mut("header")
                    .ok_or(Error::FindObs(
                        "couldn't find header field on tseries".to_string(),
                    ))?
                    .take(),
                time,
            )?;
            Ok((obs, lat_lon_elev))
        })
        .collect::<Result<Vec<(FrostObs, FrostLatLonElev)>, Error>>()?;

    Ok(data)
}

fn parse_polygon(polygon: Vec<GeoPoint>) -> String {
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
    s
}

fn json_to_spatial_cache(
    resp: serde_json::Value,
    timestamp: DateTime<Utc>,
) -> Result<SpatialCache, Error> {
    let data = extract_data(resp, timestamp)?;

    let lats: Vec<f32> = data.iter().map(|d| d.1.latitude).collect();
    let lons: Vec<f32> = data.iter().map(|d| d.1.longitude).collect();
    let elevs: Vec<f32> = data.iter().map(|d| d.1.elevation).collect();
    let values: Vec<f32> = data.iter().map(|d| d.0.body.value).collect();

    Ok(SpatialCache::new(lats, lons, elevs, values))
}

pub async fn get_spatial_data_inner(
    polygon: Vec<GeoPoint>,
    data_id: &str,
    timestamp: Timestamp,
) -> Result<SpatialCache, data_switch::Error> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let elementids: String = (&data_id).to_string();
    let time = Utc.timestamp_opt(timestamp.0, 0).unwrap();

    // Parse the vector of geopoints into an appropriate string
    let polygon_string = parse_polygon(polygon);

    let resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("polygon", polygon_string),
            ("elementids", elementids),
            ("incobs", "true".to_string()),
            (
                "time",
                (time)
                    .to_rfc3339_opts(SecondsFormat::Secs, true)
                    .to_string(),
            ),
            ("geopostype", "stationary".to_string()),
        ])
        .send()
        .await
        .map_err(|e| data_switch::Error::Other(Box::new(Error::Request(e))))?
        .json()
        .await
        .map_err(|e| data_switch::Error::Other(Box::new(Error::Request(e))))?;

    // TODO: send this part to rayon?
    json_to_spatial_cache(resp, time).map_err(|e| data_switch::Error::Other(Box::new(e)))
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

        let spatial_cache =
            json_to_spatial_cache(resp, Utc.with_ymd_and_hms(2023, 6, 30, 12, 0, 0).unwrap())
                .unwrap();

        assert_eq!(spatial_cache.data.len(), 3);
    }
}
