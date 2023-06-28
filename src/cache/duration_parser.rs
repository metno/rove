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

pub fn parse_duration(input: &str) -> IResult<&str, RelDuration> {
    let (input, _) = tag("P")(input)?;

    let (input, (years, months, _days)) = tuple((
        opt(terminated(nom::character::complete::u32, tag("Y"))),
        opt(terminated(nom::character::complete::u32, tag("M"))),
        opt(terminated(nom::character::complete::u32, tag("D"))),
    ))(input)?;

    println!("{}", input);

    Ok((
        input,
        RelDuration {
            years: years.unwrap_or_default(),
            months: months.unwrap_or_default(),
        },
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
                RelDuration {
                    years: 1,
                    months: 0
                }
            )),
        );
    }
}
