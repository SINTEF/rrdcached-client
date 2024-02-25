use nom::{
    bytes::complete::{tag, take_until1},
    character::complete::{i64 as parse_i64, newline, not_line_ending, space1, u64 as parse_u64},
    multi::separated_list1,
    number::complete::double,
    sequence::{terminated, tuple},
    IResult,
};

use crate::errors::RRDCachedClientError;

pub fn parse_response_line(input: &str) -> Result<(i64, &str), RRDCachedClientError> {
    let parse_result: IResult<&str, (i64, &str)> = tuple((
        terminated(parse_i64, space1),
        terminated(not_line_ending, newline),
    ))(input);

    match parse_result {
        Ok((_, (code, message))) => Ok((code, message)),
        Err(_) => Err(RRDCachedClientError::Parsing("parse error".to_string())),
    }
}

pub fn parse_queue_line(input: &str) -> Result<(&str, usize), RRDCachedClientError> {
    let parse_result: IResult<&str, (u64, &str)> = tuple((
        terminated(parse_u64, space1),
        terminated(not_line_ending, newline),
    ))(input);

    match parse_result {
        Ok((_, (code, message))) => Ok((message, code as usize)),
        Err(_) => Err(RRDCachedClientError::Parsing("parse error".to_string())),
    }
}

pub fn parse_stats_line(input: &str) -> Result<(&str, i64), RRDCachedClientError> {
    // name, : , at least one whitespace, number, newline
    let parse_result: IResult<&str, (&str, &str, &str, i64)> = tuple((
        take_until1(":"),
        tag(":"),
        space1,
        terminated(parse_i64, newline),
    ))(input);

    match parse_result {
        Ok((_, (name, _, _, value))) => Ok((name, value)),
        Err(_) => Err(RRDCachedClientError::Parsing("parse error".to_string())),
    }
}

pub fn parse_timestamp(input: &str) -> Result<usize, RRDCachedClientError> {
    let parse_result: IResult<&str, u64> = parse_u64(input);
    match parse_result {
        Ok((_, timestamp)) => Ok(timestamp as usize),
        Err(_) => Err(RRDCachedClientError::Parsing("parse error".to_string())),
    }
}

pub fn parse_fetch_header_line(input: &str) -> Result<(String, String), RRDCachedClientError> {
    let parse_result: IResult<&str, (&str, &str, &str, &str)> = tuple((
        take_until1(":"),
        tag(":"),
        space1,
        terminated(not_line_ending, newline),
    ))(input);

    match parse_result {
        Ok((_, (name, _tag, _space, value))) => Ok((name.to_string(), value.to_string())),
        Err(_) => Err(RRDCachedClientError::Parsing("parse error".to_string())),
    }
}

pub fn parse_fetch_line(input: &str) -> IResult<&str, (usize, Vec<f64>)> {
    tuple((
        parse_u64,
        tag(":"),
        space1,
        separated_list1(space1, double),
        newline,
    ))(input)
    .map(|(i, (timestamp, _, _, values, _))| (i, (timestamp as usize, values)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_response_line() {
        let input = "1234  hello world\n";
        let result = parse_response_line(input);
        assert_eq!(result.unwrap(), (1234, "hello world"));

        let input = "1234  hello world";
        let result = parse_response_line(input);
        assert!(result.is_err());

        let input = "0 PONG\n";
        let result = parse_response_line(input);
        assert_eq!(result.unwrap(), (0, "PONG"));

        let input = "-20 errors, a lot of errors\n";
        let result = parse_response_line(input);
        assert_eq!(result.unwrap(), (-20, "errors, a lot of errors"));

        let input = "";
        let result = parse_response_line(input);
        assert!(result.is_err());

        let input = "1234";
        let result = parse_response_line(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_queue_line() {
        let input = "12  test.rrd\n";
        let result = parse_queue_line(input);
        assert_eq!(result.unwrap(), ("test.rrd", 12));

        let input = "-0  test/test.rrd";
        let result = parse_queue_line(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_stats_line() {
        let input = "uptime: 1234\n";
        let result = parse_stats_line(input);
        assert_eq!(result.unwrap(), ("uptime", 1234));

        let input = "uptime: 1234";
        let result = parse_stats_line(input);
        assert!(result.is_err());

        let input = "upti:me:\n 1234\n";
        let result = parse_stats_line(input);
        assert!(result.is_err());

        let input = " upti:me: 1234\n";
        let result = parse_stats_line(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp() {
        let input = "1234";
        let result = parse_timestamp(input);
        assert_eq!(result.unwrap(), 1234);

        let input = "abcd\n";
        let result = parse_timestamp(input);
        assert!(result.is_err());
    }
    /*
    FlushVersion: 1
    Start: 1708800030
    Step: 10
    DSCount: 2
    DSName: ds1 ds2
    1708800040: nan nan
    1708800050: nan nan
    1708800060: nan nan
    1708800070: nan nan
    1708800080: nan nan
    */
    #[test]
    fn test_parse_fetch_header_line() {
        let input = "FlushVersion: 1\n";
        let result = parse_fetch_header_line(input);
        assert_eq!(
            result.unwrap(),
            ("FlushVersion".to_string(), "1".to_string())
        );

        let input = "FlushVersion: 1";
        let result = parse_fetch_header_line(input);
        assert!(result.is_err());

        let input = "DSName: ds1 ds2\n";
        let result = parse_fetch_header_line(input);
        assert_eq!(
            result.unwrap(),
            ("DSName".to_string(), "ds1 ds2".to_string())
        );

        let input = "0 PONG\n";
        let result = parse_fetch_header_line(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_fetch_line() {
        let input = "1708800040: nan nan\n";
        let result = parse_fetch_line(input).unwrap().1;
        assert_eq!(result.0, 1708800040);
        assert_eq!(result.1.len(), 2);
        assert!(result.1.iter().all(|f| f.is_nan()));

        let input = "1708800040: 4.2 100000\n";
        let result = parse_fetch_line(input);
        assert_eq!(result.unwrap().1, (1708800040, vec![4.2, 100000.0]));

        let input = "1708800040: nan nan";
        let result = parse_fetch_line(input);
        assert!(result.is_err());

        let input = "End: 1708886440";
        let result = parse_fetch_line(input);
        assert!(result.is_err());
    }
}
