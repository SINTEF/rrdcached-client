use crate::{errors::RRDCachedClientError, now::now_timestamp, sanitisation::check_rrd_path};

pub struct BatchUpdate {
    path: String,
    timestamp: Option<usize>,
    data: Vec<f64>,
}

impl BatchUpdate {
    pub fn new(
        path: &str,
        timestamp: Option<usize>,
        data: Vec<f64>,
    ) -> Result<BatchUpdate, RRDCachedClientError> {
        if data.is_empty() {
            return Err(RRDCachedClientError::InvalidBatchUpdate(
                "data is empty".to_string(),
            ));
        }
        check_rrd_path(path)?;
        Ok(BatchUpdate {
            path: path.to_string(),
            timestamp,
            data,
        })
    }

    pub fn to_command_string(&self) -> Result<String, RRDCachedClientError> {
        let timestamp_str = match self.timestamp {
            Some(ts) => ts.to_string(),
            None => now_timestamp()?.to_string(),
        };
        let data_str = self
            .data
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<String>>()
            .join(":");
        let mut command = String::with_capacity(
            7 + self.path.len() + 5 + timestamp_str.len() + 1 + data_str.len() + 1,
        );
        command.push_str("UPDATE ");
        command.push_str(&self.path);
        command.push_str(".rrd ");
        command.push_str(&timestamp_str);
        command.push(':');
        command.push_str(&data_str);
        command.push('\n');

        Ok(command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_with_valid_data() {
        let path = "valid_path";
        let timestamp = Some(123456789);
        let data = vec![1.0, 2.0, 3.0];
        let batch_update = BatchUpdate::new(path, timestamp, data).unwrap();
        assert_eq!(batch_update.path, "valid_path");
        assert_eq!(batch_update.timestamp, Some(123456789));
        assert_eq!(batch_update.data, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_new_with_empty_data() {
        let path = "valid_path";
        let timestamp = Some(123456789);
        let data = vec![];
        let result = BatchUpdate::new(path, timestamp, data);
        assert!(matches!(
            result,
            Err(RRDCachedClientError::InvalidBatchUpdate(msg)) if msg == "data is empty"
        ));
    }

    #[test]
    fn test_to_command_string_with_timestamp() {
        let batch_update = BatchUpdate {
            path: "test_path".into(),
            timestamp: Some(1609459200), // Example timestamp
            data: vec![1.1, 2.2, 3.3],
        };
        let command = batch_update.to_command_string().unwrap();
        assert_eq!(command, "UPDATE test_path.rrd 1609459200:1.1:2.2:3.3\n");
    }
}
