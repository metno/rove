use crate::frost::{duration, Error, FrostLatLonElev, FrostLocation};
use chrono::prelude::*;
use chronoutil::RelativeDuration;

pub fn extract_duration(header: &mut serde_json::Value) -> Result<RelativeDuration, Error> {
    let time_resolution = header
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
            "couldn't find field timeresolution on timeseries".to_string(),
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

pub fn extract_location(
    header: &mut serde_json::Value,
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

pub fn extract_station_id(header: &mut serde_json::Value) -> Result<String, Error> {
    let station_id: i32 = serde_json::from_value(
        header
            .get_mut("id")
            .ok_or(Error::FindMetadata(
                "couldn't find id in header".to_string(),
            ))?
            .get_mut("stationid")
            .ok_or(Error::FindMetadata(
                "couldn't find stationid field in id".to_string(),
            ))?
            .take(),
    )?;

    Ok(station_id.to_string())
}
