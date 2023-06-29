use chrono::Duration;
use chronoutil::RelativeDuration;
use nom::{
    bytes::complete::tag,
    combinator::opt,
    sequence::{preceded, terminated, tuple},
    IResult,
};

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

fn parse_datespec(datespec: &str) -> (i32, i32, i32) {
    todo!()
}

fn parse_timespec(timespec: &str) -> (i32, i32, i32) {
    todo!()
}

pub fn parse_duration_handwritten(input: &str) -> Option<Duration> {
    let input = input.strip_prefix('P')?;

    let (datespec, timespec) = input.split_once('T').unwrap_or((input, ""));

    let (years, months, days) = parse_datespec(datespec);
    let (hours, mins, secs) = parse_timespec(timespec);

    todo!()
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
