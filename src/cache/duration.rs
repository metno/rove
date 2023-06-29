use nom::{
    bytes::complete::tag,
    combinator::opt,
    sequence::{terminated, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub struct RelDuration {
    years: u32,
    months: u32,
}

fn parse_date(input: &str) -> IResult<&str, (u32, u32, u32)> {
    let (input, (years, months, days)) = tuple((
        opt(terminated(nom::character::complete::u32, tag("Y"))),
        opt(terminated(nom::character::complete::u32, tag("M"))),
        opt(terminated(nom::character::complete::u32, tag("D"))),
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

pub fn parse_duration(input: &str) -> IResult<&str, RelDuration> {
    let (input, _) = tag("P")(input)?;

    let (input, (years, months, _days)) = parse_date(input)?;

    println!("{}", input);

    Ok((input, RelDuration { years, months }))
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
                RelDuration {
                    years: 1,
                    months: 0
                }
            )),
        );
    }
}
