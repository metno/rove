use chrono::Duration;
use chronoutil::RelativeDuration;
use nom::{
    bytes::complete::tag,
    combinator::opt,
    sequence::{terminated, tuple},
    IResult,
};

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

pub fn parse_duration(input: &str) -> IResult<&str, RelativeDuration> {
    let (input, _) = tag("P")(input)?;

    let (input, (years, months, _days)) = parse_date(input)?;

    println!("{}", input);

    Ok((
        input,
        RelativeDuration::months(years * 12 + months).with_duration(Duration::seconds(0)),
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
                "T1S",
                RelativeDuration::months(12).with_duration(Duration::seconds(0)),
            )),
        );
    }
}
