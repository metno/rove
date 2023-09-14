use crate::{duration, Error, FrostObs};
use chrono::{prelude::*, Duration};
use chronoutil::RelativeDuration;
use rove::data_switch::{self, SeriesCache, Timerange, Timestamp};

fn extract_duration(mut metadata_resp: serde_json::Value) -> Result<RelativeDuration, Error> {
    let time_resolution = metadata_resp
        .get_mut("data")
        .ok_or(Error::FindMetadata(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(Error::FindMetadata(
            "couldn't find field tseries on data".to_string(),
        ))?
        .get_mut(0)
        .ok_or(Error::FindMetadata("tseries array is empty".to_string()))?
        .get_mut("header")
        .ok_or(Error::FindMetadata(
            "couldn't find field header on 1st tseries".to_string(),
        ))?
        .get_mut("extra")
        .ok_or(Error::FindMetadata(
            "couldn't find field extra on header".to_string(),
        ))?
        .get_mut("timeseries")
        .ok_or(Error::FindMetadata(
            "couldn't find field timeseries on extra".to_string(),
        ))?
        .get_mut("timeresolution")
        .ok_or(Error::FindMetadata(
            "couldn't find field timeresolution on quality".to_string(),
        ))?
        .as_str()
        .ok_or(Error::FindMetadata(
            "field timeresolution was not a string".to_string(),
        ))?;

    duration::parse_duration(time_resolution).map_err(|e| Error::ParseDuration {
        source: e,
        input: time_resolution.to_string(),
    })
}

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

fn json_to_series_cache(
    resp: serde_json::Value,
    period: RelativeDuration,
    num_leading_points: u8,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<SeriesCache, Error> {
    let obses: Vec<FrostObs> = extract_obs(resp)?;

    // TODO: preallocate?
    // let ts_length = (end_time - first_obs_time) / period;
    let mut data = Vec::new();

    let mut curr_obs_time = interval_start - period * i32::from(num_leading_points);
    let first_obs_time = obses
        .first()
        .ok_or(Error::MissingObs(
            "obs array from frost is empty".to_string(),
        ))?
        .time;

    // handle misalignment of interval_start with ts, and leading missing values
    if curr_obs_time != first_obs_time {
        if first_obs_time < curr_obs_time {
            return Err(Error::Misalignment(
                "the first obs returned by frost is outside the time range".to_string(),
            ));
        }

        while first_obs_time >= curr_obs_time + period {
            data.push(None);
            curr_obs_time = curr_obs_time + period;
        }

        curr_obs_time = first_obs_time;
    }

    let num_preceding_nones = i32::try_from(data.len()).map_err(|_| {
        Error::MissingObs("i32 overflow: too many missing obs at start".to_string())
    })?;
    let start_time = Timestamp((first_obs_time - period * num_preceding_nones).timestamp());

    // insert obses into data, with Nones for gaps in the series
    for obs in obses {
        if curr_obs_time == obs.time {
            data.push(Some(obs.body.value));
            curr_obs_time = curr_obs_time + period;
        } else {
            while curr_obs_time < obs.time {
                data.push(None);
                curr_obs_time = curr_obs_time + period;
            }
            if curr_obs_time == obs.time {
                data.push(Some(obs.body.value));
                curr_obs_time = curr_obs_time + period;
            } else {
                return Err(Error::Misalignment(
                    "obs misaligned with series".to_string(),
                ));
            }
        }
    }

    // handle trailing missing values
    while curr_obs_time < interval_end {
        data.push(None);
        curr_obs_time = curr_obs_time + period;
    }

    Ok(SeriesCache {
        start_time,
        period,
        data,
        num_leading_points,
    })
}

pub async fn get_series_data_inner(
    data_id: &str,
    timerange: Timerange,
    num_leading_points: u8,
) -> Result<SeriesCache, data_switch::Error> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let (station_id, element_id) =
        data_id
            .split_once('/')
            .ok_or(data_switch::Error::InvalidDataId {
                data_id: data_id.to_string(),
                data_source: "frost",
                source: Box::new(Error::InvalidDataId(
                    "no \"/\" found to separate station_id from element_id",
                )),
            })?;

    // TODO: should these maybe just be passed in this way?
    let interval_start = Utc.timestamp_opt(timerange.start.0, 0).unwrap();
    let interval_end = Utc.timestamp_opt(timerange.end.0, 0).unwrap();

    let metadata_resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", element_id),
            ("stationids", station_id),
            ("incobs", "false"),
        ])
        .send()
        .await
        .map_err(|e| data_switch::Error::Other(Box::new(Error::Request(e))))?
        .json()
        .await
        .map_err(|e| data_switch::Error::Other(Box::new(Error::Request(e))))?;

    let period =
        extract_duration(metadata_resp).map_err(|e| data_switch::Error::Other(Box::new(e)))?;

    let resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", element_id),
            ("stationids", station_id),
            ("incobs", "true"),
            (
                "time",
                format!(
                    "{}/{}",
                    (interval_start - period * i32::from(num_leading_points))
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                    (interval_end + Duration::seconds(1))
                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                )
                .as_str(),
            ),
        ])
        .send()
        .await
        .map_err(|e| data_switch::Error::Other(Box::new(Error::Request(e))))?
        .json()
        .await
        .map_err(|e| data_switch::Error::Other(Box::new(Error::Request(e))))?;

    // TODO: send this part to rayon?
    json_to_series_cache(
        resp,
        period,
        num_leading_points,
        interval_start,
        interval_end,
    )
    .map_err(|e| data_switch::Error::Other(Box::new(e)))
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
            "element": {
              "description": "Air temperature (default 2 m above ground), present value",
              "id": "air_temperature",
              "name": "Air temperature",
              "unit": "degC"
            },
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
                    "from": "1931-01-01T06:00:00Z",
                    "to": "1931-01-01T06:00:00Z",
                    "value": "1"
                  }
                ],
                "performance": [
                  {
                    "from": "1937-01-01T06:00:00Z",
                    "to": "1937-01-01T06:00:00Z",
                    "value": "unknown"
                  }
                ]
              },
              "timeoffset": "PT0H",
              "timeresolution": "PT1H"
            }
          },
          "available": {
            "from": "1937-01-01T06:00:00Z"
          }
        },
        "observations": [
          {
            "time": "2023-06-26T12:00:00Z",
            "body": {
              "qualitycode": "0",
              "value": "27.3999996"
            }
          },
          {
            "time": "2023-06-26T13:00:00Z",
            "body": {
              "qualitycode": "0",
              "value": "25.7999992"
            }
          },
          {
            "time": "2023-06-26T14:00:00Z",
            "body": {
              "qualitycode": "0",
              "value": "26"
            }
          }
        ]
      }
    ]
  }
}"#;

    #[test]
    fn test_json_to_series_cache() {
        let resp = serde_json::from_str(RESP).unwrap();

        let series_cache = json_to_series_cache(
            resp,
            RelativeDuration::hours(1),
            2,
            Utc.with_ymd_and_hms(2023, 6, 26, 14, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 6, 26, 14, 0, 0).unwrap(),
        )
        .unwrap();

        assert_eq!(
            Utc.timestamp_opt(series_cache.start_time.0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 6, 26, 12, 0, 0).unwrap(),
        );
        assert_eq!(
            series_cache.data,
            vec![Some(27.3999996), Some(25.7999992), Some(26.)]
        );
    }
}
