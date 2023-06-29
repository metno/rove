use chrono::Duration;
use chronoutil::RelativeDuration;
use nom::{
    bytes::complete::tag,
    combinator::opt,
    sequence::{terminated, tuple},
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
    let (input, _) = tag("P")(input)?;

    let (input, (years, months, days)) = parse_date(input)?;

    let (input, _) = tag("T")(input)?;

    let (input, (hours, minutes, seconds)) = parse_time(input)?;

    println!("{}", input);

    Ok((
        input,
        RelativeDuration::months(years * 12 + months)
            .with_duration(dhms_to_duration(days, hours, minutes, seconds)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration("P1YT1S"),
            Ok((
                "",
                RelativeDuration::months(12).with_duration(Duration::seconds(1)),
            )),
        );
    }
}
