use chrono::Duration;
use chronoutil::RelativeDuration;
use nom::{
    bytes::complete::tag,
    combinator::opt,
    sequence::{preceded, terminated, tuple},
    IResult,
};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("failed to parse duration because: {0}")]
    Parse(String),
}

fn dhms_to_duration(days: i32, hours: i32, minutes: i32, seconds: i32) -> Duration {
    Duration::seconds((((days * 24 + hours) * 60 + minutes) * 60 + seconds) as i64)
}

fn parse_date(input: &str) -> IResult<&str, (i32, i32, i32)> {
    let (input, (years, months, days)) = tuple((
        opt(terminated(nom::character::complete::i32, tag("Y"))),
        opt(terminated(nom::character::complete::i32, tag("M"))),
        opt(terminated(nom::character::complete::i32, tag("D"))),
    ))(input)?;

    Ok((
        input,
        (
            years.unwrap_or_default(),
            months.unwrap_or_default(),
            days.unwrap_or_default(),
        ),
    ))
}

fn parse_time(input: &str) -> IResult<&str, (i32, i32, i32)> {
    let (input, (hours, minutes, seconds)) = tuple((
        opt(terminated(nom::character::complete::i32, tag("H"))),
        opt(terminated(nom::character::complete::i32, tag("M"))),
        opt(terminated(nom::character::complete::i32, tag("S"))),
    ))(input)?;

    Ok((
        input,
        (
            hours.unwrap_or_default(),
            minutes.unwrap_or_default(),
            seconds.unwrap_or_default(),
        ),
    ))
}

pub fn parse_duration(input: &str) -> IResult<&str, RelativeDuration> {
    let (input, ((years, months, days), time)) = preceded(
        tag("P"),
        tuple((parse_date, opt(preceded(tag("T"), parse_time)))),
    )(input)?;

    let (hours, minutes, seconds) = time.unwrap_or_default();

    Ok((
        input,
        RelativeDuration::months(years * 12 + months)
            .with_duration(dhms_to_duration(days, hours, minutes, seconds)),
    ))
}

fn get_terminated(input: &str, terminator: char) -> Result<(&str, i32), Error> {
    if let Some((int_string, remainder)) = input.split_once(terminator) {
        let int = int_string
            .parse::<i32>()
            .map_err(|_| Error::Parse(format!("{} is not a valid i32", int_string)))?;
        Ok((remainder, int))
    } else {
        Ok((input, 0))
    }
}

fn parse_datespec(datespec: &str) -> Result<(i32, i32, i32), Error> {
    let (remainder, years) = get_terminated(datespec, 'Y')?;
    let (remainder, months) = get_terminated(remainder, 'M')?;
    let (remainder, days) = get_terminated(remainder, 'D')?;

    if remainder.len() != 0 {
        Err(Error::Parse(format!(
            "trailing characters: {} in datespec: {}",
            remainder, datespec
        )))
    } else {
        Ok((years, months, days))
    }
}

fn parse_timespec(timespec: &str) -> Result<(i32, i32, i32), Error> {
    let (remainder, hours) = get_terminated(timespec, 'H')?;
    let (remainder, mins) = get_terminated(remainder, 'M')?;
    let (remainder, secs) = get_terminated(remainder, 'S')?;

    if remainder.len() != 0 {
        Err(Error::Parse(format!(
            "trailing characters: {} in timespec: {}",
            remainder, timespec
        )))
    } else {
        Ok((hours, mins, secs))
    }
}

pub fn parse_duration_handwritten(input: &str) -> Result<RelativeDuration, Error> {
    let input = input
        .strip_prefix('P')
        .ok_or_else(|| Error::Parse("duration was not prefixed with P".to_string()))?;

    let (datespec, timespec) = input.split_once('T').unwrap_or((input, ""));

    let (years, months, days) = parse_datespec(datespec)?;
    let (hours, mins, secs) = parse_timespec(timespec)?;

    Ok(RelativeDuration::months(years * 12 + months)
        .with_duration(dhms_to_duration(days, hours, mins, secs)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        [
            (
                "P1YT1S",
                RelativeDuration::months(12).with_duration(Duration::seconds(1)),
            ),
            (
                "P2Y2M2DT2H2M2S",
                RelativeDuration::months(2 * 12 + 2).with_duration(dhms_to_duration(2, 2, 2, 2)),
            ),
            (
                "P1M",
                RelativeDuration::months(1).with_duration(Duration::zero()),
            ),
            ("PT10M", RelativeDuration::minutes(10)),
        ]
        .into_iter()
        .for_each(|(input, expected)| assert_eq!(parse_duration(input), Ok(("", expected))))
    }
}
