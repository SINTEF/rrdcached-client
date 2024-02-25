use thiserror::Error;

#[derive(Error, Debug)]
pub enum RRDCachedClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parsing error: {0}")]
    Parsing(String),
    #[error("unexpected response {0}: {1}")]
    UnexpectedResponse(i64, String),
    #[error("Invalid create data serie: {0}")]
    InvalidCreateDataSerie(String),
    #[error("Invalid data source name: {0}")]
    InvalidDataSourceName(String),
    #[error("Invalid batch update: {0}")]
    InvalidBatchUpdate(String),
    #[error("Batch Update Error Response: {0}")]
    BatchUpdateErrorResponse(String, Vec<String>),
    #[error("Unable to get system time")]
    SystemTimeError,
    #[error("Invalid fetch: {0}")]
    InvalidFetch(String),
    #[error("Invalid fetch header line: {0}")]
    InvalidFetchHeaderLine(String),
}
