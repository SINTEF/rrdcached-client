use crate::{
    errors::RRDCachedClientError,
    parsers::{parse_fetch_header_line, parse_fetch_line},
};

#[derive(Debug, PartialEq)]
pub struct FetchResponse {
    pub flush_version: u32,
    pub start: usize,
    pub end: usize,
    pub step: usize,
    pub ds_count: usize,
    pub ds_names: Vec<String>,
    pub data: Vec<(usize, Vec<f64>)>,
}

impl FetchResponse {
    pub fn from_lines(lines: Vec<String>) -> Result<FetchResponse, RRDCachedClientError> {
        let mut flush_version = None;
        let mut start = None;
        let mut end = None;
        let mut step = None;
        let mut ds_count = None;
        let mut ds_names = None;
        let mut data: Vec<(usize, Vec<f64>)> = Vec::new();

        let mut index_data_start = None;
        for (index, line) in lines.iter().enumerate() {
            let (key, value) = parse_fetch_header_line(line)?;
            match key.as_str() {
                "FlushVersion" => {
                    flush_version = Some(value.parse().map_err(|_| {
                        RRDCachedClientError::Parsing("Unable to parse flush version".to_string())
                    })?);
                }
                "Start" => {
                    start = Some(value.parse().map_err(|_| {
                        RRDCachedClientError::Parsing("Unable to parse start".to_string())
                    })?);
                }
                "End" => {
                    end = Some(value.parse().map_err(|_| {
                        RRDCachedClientError::Parsing("Unable to parse end".to_string())
                    })?);
                }
                "Step" => {
                    step = Some(value.parse().map_err(|_| {
                        RRDCachedClientError::Parsing("Unable to parse step".to_string())
                    })?);
                }
                "DSCount" => {
                    ds_count = Some(value.parse().map_err(|_| {
                        RRDCachedClientError::Parsing("Unable to parse ds count".to_string())
                    })?);
                }
                "DSName" => {
                    ds_names = Some(value.split_whitespace().map(|s| s.to_string()).collect());
                }
                _ => match parse_fetch_line(line) {
                    Ok((_, (timestamp, values))) => {
                        data.push((timestamp, values));
                        index_data_start = Some(index);
                        break;
                    }
                    Err(_) => {
                        return Err(RRDCachedClientError::InvalidFetchHeaderLine(
                            line.to_string(),
                        ));
                    }
                },
            }
        }

        if let Some(index_data_start) = index_data_start {
            for line in lines.iter().skip(index_data_start + 1) {
                match parse_fetch_line(line) {
                    Ok((_, (timestamp, values))) => {
                        data.push((timestamp, values));
                    }
                    Err(_) => {
                        return Err(RRDCachedClientError::InvalidFetch(line.to_string()));
                    }
                }
            }
        }

        Ok(FetchResponse {
            flush_version: flush_version.unwrap_or(0),
            start: start.unwrap_or(0),
            end: end.unwrap_or(0),
            step: step.unwrap_or(0),
            ds_count: ds_count.unwrap_or(0),
            ds_names: ds_names.unwrap_or(Vec::new()),
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_successful_parse() {
        let input = vec![
            "FlushVersion: 1\n".to_string(),
            "Start: 1708800030\n".to_string(),
            "End: 1708886440\n".to_string(),
            "Step: 10\n".to_string(),
            "DSCount: 2\n".to_string(),
            "DSName: ds1 ds2\n".to_string(),
            "1708800040: 1 2\n".to_string(),
            "1708800050: 3 3\n".to_string(),
        ];

        let expected = FetchResponse {
            flush_version: 1,
            start: 1708800030,
            end: 1708886440,
            step: 10,
            ds_count: 2,
            ds_names: vec!["ds1".to_string(), "ds2".to_string()],
            data: vec![(1708800040, vec![1.0, 2.0]), (1708800050, vec![3.0, 3.0])],
        };

        let result = FetchResponse::from_lines(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_error_numbers() {
        let input = vec![
            "FlushVersion: xyz\n".to_string(), // Incorrect format
        ];

        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());

        let input = vec![
            "Start: xyz\n".to_string(), // Incorrect format
        ];
        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());

        let input = vec![
            "End: xyz\n".to_string(), // Incorrect format
        ];
        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());

        let input = vec![
            "Step: xyz\n".to_string(), // Incorrect format
        ];
        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());

        let input = vec![
            "DSCount: xyz\n".to_string(), // Incorrect format
        ];
        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_incomplete_data() {
        let input = vec![
            "FlushVersion: 1\n".to_string(),
            // Missing "Start", "End", "Step", "DSCount", "DSName"
            "1708800040: 1.0 2.0\n".to_string(),
        ];

        // Expect defaults for missing fields
        let expected = FetchResponse {
            flush_version: 1,
            start: 0,             // Default due to missing
            end: 0,               // Default due to missing
            step: 0,              // Default due to missing
            ds_count: 0,          // Default due to missing
            ds_names: Vec::new(), // Default due to missing
            data: vec![(1708800040, vec![1.0, 2.0])],
        };

        let result = FetchResponse::from_lines(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_input() {
        let input: Vec<String> = vec![];

        let _ = FetchResponse::from_lines(input).unwrap();
    }

    #[test]
    fn test_no_data_lines() {
        let input = vec![
            "FlushVersion: 1\n".to_string(),
            "DSName: ds1 ds2\n".to_string(),
            // No data lines
        ];

        // Expected behavior could vary, this is just an example
        let _ = FetchResponse::from_lines(input).unwrap();
    }

    #[test]
    fn test_valid_header_invalid_data() {
        let input = vec![
            "FlushVersion: 1\n".to_string(),
            "1708800040: abc def\n".to_string(),
        ];

        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());

        let input = vec![
            "FlushVersion: 1\n".to_string(),
            "1708800040: 1.0\n".to_string(), // Missing second value
            "1708800040: abc\n".to_string(), // Missing second value
            "1708800040: 2.0\n".to_string(), // Missing second value
        ];

        let result = FetchResponse::from_lines(input);
        assert!(result.is_err());
    }
}
