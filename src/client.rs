use crate::batch_update::BatchUpdate;
use crate::consolidation_function::ConsolidationFunction;
use crate::create::*;
use crate::errors::RRDCachedClientError;
use crate::fetch::FetchResponse;
use crate::parsers::*;
use crate::sanitisation::check_rrd_path;
use std::collections::HashMap;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::{io::BufReader, net::TcpStream};

/// A client to interact with a RRDCached server.
#[derive(Debug)]
pub struct RRDCachedClient<T = TcpStream> {
    stream: BufReader<T>,
}

impl RRDCachedClient<TcpStream> {
    /// Connect to a RRDCached server over TCP.
    pub async fn connect_tcp(addr: &str) -> Result<Self, RRDCachedClientError> {
        let stream = TcpStream::connect(addr).await?;
        let stream = BufReader::new(stream);
        Ok(Self { stream })
    }
}

impl RRDCachedClient<UnixStream> {
    /// Connect to a RRDCached server over a Unix socket.
    pub async fn connect_unix(addr: &str) -> Result<Self, RRDCachedClientError> {
        let stream = UnixStream::connect(addr).await?;
        let stream = BufReader::new(stream);
        Ok(Self { stream })
    }
}

impl<T> RRDCachedClient<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn assert_response_code(&self, code: i64, message: &str) -> Result<(), RRDCachedClientError> {
        if code < 0 {
            Err(RRDCachedClientError::UnexpectedResponse(
                code,
                message.to_string(),
            ))
        } else {
            Ok(())
        }
    }

    async fn read_line(&mut self) -> Result<String, RRDCachedClientError> {
        let mut line = String::new();
        self.stream.read_line(&mut line).await?;
        Ok(line)
    }

    async fn read_n_lines(&mut self, n: usize) -> Result<Vec<String>, RRDCachedClientError> {
        let mut lines = Vec::with_capacity(n);
        for _ in 0..n {
            lines.push(self.read_line().await?);
        }
        Ok(lines)
    }

    async fn send_command(
        &mut self,
        command: &str,
    ) -> Result<(usize, String), RRDCachedClientError> {
        // Send command
        self.stream.write_all(command.as_bytes()).await?;
        // Read response
        let response_line = self.read_line().await?;
        let (code, message) = parse_response_line(&response_line)?;
        self.assert_response_code(code, message)?;
        let nb_lines = usize::try_from(code).map_err(|_| {
            RRDCachedClientError::UnexpectedResponse(code, "invalid number of lines".to_string())
        })?;
        Ok((nb_lines, message.to_string()))
    }

    /// Retreive documentation for humans.
    ///
    /// Can take an optional command to get help for a specific command.
    pub async fn help(
        &mut self,
        command: Option<&str>,
    ) -> Result<(String, Vec<String>), RRDCachedClientError> {
        let command = match command {
            Some(command) => {
                let mut a = String::with_capacity(6 + command.len());
                a.push_str("HELP ");
                a.push_str(command);
                a.push('\n');
                a
            }
            None => "HELP\n".to_string(),
        };
        let (nb_lines, header) = self.send_command(&command).await?;
        let lines = self.read_n_lines(nb_lines).await?;

        Ok((header, lines))
    }

    /// Ping the server to check if it's alive.
    pub async fn ping(&mut self) -> Result<(), RRDCachedClientError> {
        let (_, message) = self.send_command("PING\n").await?;
        assert!(message == "PONG");
        Ok(())
    }

    /// Create a new RRD
    pub async fn create(&mut self, arguments: CreateArguments) -> Result<(), RRDCachedClientError> {
        let arguments_str = arguments.to_str();
        let mut command = String::with_capacity(7 + arguments_str.len());
        command.push_str("CREATE ");
        command.push_str(&arguments_str);
        command.push('\n');
        let (_, message) = self.send_command(&command).await?;
        if message != "RRD created OK" {
            return Err(RRDCachedClientError::UnexpectedResponse(
                0,
                message.to_string(),
            ));
        }
        Ok(())
    }

    /// Flush a RRD
    pub async fn flush(&mut self, path: &str) -> Result<(), RRDCachedClientError> {
        check_rrd_path(path)?;
        let mut command = String::with_capacity(6 + path.len() + 5);
        command.push_str("FLUSH ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let _ = self.send_command(&command).await?;
        Ok(())
    }

    /// Flush all RRDs
    pub async fn flush_all(&mut self) -> Result<(), RRDCachedClientError> {
        let _ = self.send_command("FLUSHALL\n").await?;
        Ok(())
    }

    /// Pending updates
    pub async fn pending(&mut self, path: &str) -> Result<Vec<String>, RRDCachedClientError> {
        check_rrd_path(path)?;
        let mut command = String::with_capacity(7 + path.len() + 5);
        command.push_str("PENDING ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let (nb_lines, _) = self.send_command(&command).await?;
        if nb_lines > 0 {
            let lines = self.read_n_lines(nb_lines).await?;
            Ok(lines)
        } else {
            Ok(vec![])
        }
    }

    /// Forget pending updates
    pub async fn forget(&mut self, path: &str) -> Result<(), RRDCachedClientError> {
        check_rrd_path(path)?;
        let mut command = String::with_capacity(7 + path.len() + 5);
        command.push_str("FORGET ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let _ = self.send_command(&command).await?;
        Ok(())
    }

    /// Get the queue information
    pub async fn queue(&mut self) -> Result<Vec<(String, usize)>, RRDCachedClientError> {
        let (nb_lines, _message) = self.send_command("QUEUE\n").await?;
        let nb_lines = self.read_n_lines(nb_lines).await?;
        let parsed_lines = nb_lines
            .iter()
            .map(|line| {
                let (path, pending) = parse_queue_line(line)?;
                Ok((path.to_string(), pending))
            })
            .collect::<Result<Vec<(String, usize)>, RRDCachedClientError>>()?;
        Ok(parsed_lines)
    }

    /// Get the server stats
    pub async fn stats(&mut self) -> Result<HashMap<String, i64>, RRDCachedClientError> {
        let (nb_lines, _message) = self.send_command("STATS\n").await?;
        let lines = self.read_n_lines(nb_lines).await?;
        let parsed_lines = lines
            .iter()
            .map(|line| {
                let (name, value) = parse_stats_line(line)?;
                Ok((name.to_string(), value))
            })
            .collect::<Result<HashMap<String, i64>, RRDCachedClientError>>()?;
        Ok(parsed_lines)
    }

    /// Get the first CDP (whatever that is)
    pub async fn first(
        &mut self,
        path: &str,
        round_robin_archive: Option<usize>,
    ) -> Result<usize, RRDCachedClientError> {
        check_rrd_path(path)?;
        let round_robin_archive = round_robin_archive.unwrap_or(0);
        let rranum_str = round_robin_archive.to_string();

        let mut command = String::with_capacity(6 + path.len() + 5 + rranum_str.len() + 1);
        command.push_str("FIRST ");
        command.push_str(path);
        command.push_str(".rrd ");
        command.push_str(&rranum_str);
        command.push('\n');
        let (_, message) = self.send_command(&command).await?;
        let timestamp = parse_timestamp(&message)?;
        Ok(timestamp)
    }

    /// Retrieve the last update timestamp
    pub async fn last(&mut self, path: &str) -> Result<usize, RRDCachedClientError> {
        check_rrd_path(path)?;

        let mut command = String::with_capacity(5 + path.len() + 5);
        command.push_str("LAST ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let (_, message) = self.send_command(&command).await?;
        let timestamp = parse_timestamp(&message)?;
        Ok(timestamp)
    }

    /// Retreive information about a RRD
    pub async fn info(&mut self, path: &str) -> Result<Vec<String>, RRDCachedClientError> {
        check_rrd_path(path)?;
        let mut command = String::with_capacity(5 + path.len() + 5);
        command.push_str("INFO ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let (nb_lines, _message) = self.send_command(&command).await?;
        let lines = self.read_n_lines(nb_lines).await?;
        Ok(lines)
    }

    /// List RRDs
    pub async fn list(
        &mut self,
        recursive: bool,
        path: Option<&str>,
    ) -> Result<Vec<String>, RRDCachedClientError> {
        let path = path.unwrap_or("/");
        let mut command =
            String::with_capacity(5 + path.len() + 1 + (if recursive { 10 } else { 0 }));
        command.push_str("LIST ");
        if recursive {
            command.push_str("RECURSIVE ");
        }
        command.push_str(path);
        command.push('\n');
        let (nb_lines, _message) = self.send_command(&command).await?;
        let lines = self.read_n_lines(nb_lines).await?;
        Ok(lines)
    }

    /// Suspend a RRD
    pub async fn suspend(&mut self, path: &str) -> Result<(), RRDCachedClientError> {
        check_rrd_path(path)?;
        let mut command = String::with_capacity(8 + path.len() + 5);
        command.push_str("SUSPEND ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let _ = self.send_command(&command).await?;
        Ok(())
    }

    /// Resume a RRD
    pub async fn resume(&mut self, path: &str) -> Result<(), RRDCachedClientError> {
        check_rrd_path(path)?;
        let mut command = String::with_capacity(7 + path.len() + 5);
        command.push_str("RESUME ");
        command.push_str(path);
        command.push_str(".rrd\n");
        let _ = self.send_command(&command).await?;
        Ok(())
    }

    /// Suspend all RRDs
    pub async fn suspend_all(&mut self) -> Result<(), RRDCachedClientError> {
        let _ = self.send_command("SUSPENDALL\n").await?;
        Ok(())
    }

    /// Resume all RRDs
    pub async fn resume_all(&mut self) -> Result<(), RRDCachedClientError> {
        let _ = self.send_command("RESUMEALL\n").await?;
        Ok(())
    }

    /// Close the connection to the server
    pub async fn quit(&mut self) -> Result<(), RRDCachedClientError> {
        // Send directly without checking the response
        self.stream.write_all("QUIT\n".as_bytes()).await?;
        Ok(())
    }

    /// Update a RRD with a list of values at a specific timestamp
    ///
    /// The order is important as it must match the order of the data sources in the RRD
    pub async fn update(
        &mut self,
        path: &str,
        timestamp: Option<usize>,
        data: Vec<f64>,
    ) -> Result<(), RRDCachedClientError> {
        let command = BatchUpdate::new(path, timestamp, data)?;
        let command_str = command.to_command_string()?;
        let _ = self.send_command(&command_str).await?;
        Ok(())
    }

    /// Update a RRD with a single value at a specific timestamp.
    ///
    /// Convenient helper when a RRD contains only one data source.
    pub async fn update_one(
        &mut self,
        path: &str,
        timestamp: Option<usize>,
        data: f64,
    ) -> Result<(), RRDCachedClientError> {
        self.update(path, timestamp, vec![data]).await
    }

    /// Batch updates.
    ///
    /// RRDCached presents this as a more efficient way to update multiple RRDs at once.
    /// You may want to sort the updates by timestamp ascending as RDDtool will
    /// reject updates of older timestamps.
    pub async fn batch(&mut self, commands: Vec<BatchUpdate>) -> Result<(), RRDCachedClientError> {
        let _ = self.send_command("BATCH\n").await?;
        for command in commands {
            let command_str = command.to_command_string()?;
            // write the command directly
            self.stream.write_all(command_str.as_bytes()).await?;
        }
        // Send a dot to end the batch
        let (nb_lines, message) = self.send_command(".\n").await?;

        // It returns errors line by line if there are any
        if nb_lines > 0 {
            let lines = self.read_n_lines(nb_lines).await?;
            return Err(RRDCachedClientError::BatchUpdateErrorResponse(
                message, lines,
            ));
        }
        Ok(())
    }

    /// Fetch the content of a Round Robin Database (RRD)
    ///
    /// Note that we use the Ascii protocol, as the binary protocol is not documented
    /// and it's unsure whether it's consitent between versions of RRDCached or
    /// system architectures.
    ///
    ///
    pub async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<FetchResponse, RRDCachedClientError> {
        check_rrd_path(path)?;
        let consolidation_function_str = consolidation_function.to_str();
        // FETCH path.rrd CF [--start start] [--end end] [--columns columns]
        let mut capacity = 6 + path.len() + 5 + consolidation_function_str.len() + 1;
        let mut start_str: Option<String> = None;
        let mut end_str: Option<String> = None;
        let mut columns_str: Option<String> = None;
        match start {
            Some(start) => {
                let start_string = start.to_string();
                capacity += 1 + start_string.len();
                start_str = Some(start_string);

                if let Some(end) = end {
                    let end_string = end.to_string();
                    capacity += 1 + end_string.len();
                    end_str = Some(end_string);
                    if let Some(columns) = columns {
                        let columns_string = columns.join(" ");
                        capacity += 1 + columns_string.len();
                        columns_str = Some(columns_string);
                    }
                } else if columns.is_some() {
                    return Err(RRDCachedClientError::InvalidFetch(
                        "end must be specified".to_string(),
                    ));
                }
            }
            None => {
                if end.is_some() || columns.is_some() {
                    return Err(RRDCachedClientError::InvalidFetch(
                        "start must be specified".to_string(),
                    ));
                }
            }
        }
        let mut command = String::with_capacity(capacity);
        command.push_str("FETCH ");
        command.push_str(path);
        command.push_str(".rrd ");
        command.push_str(consolidation_function_str);
        if let Some(start_str) = start_str {
            command.push(' ');
            command.push_str(&start_str);
            if let Some(end_str) = end_str {
                command.push(' ');
                command.push_str(&end_str);
                if let Some(columns_str) = columns_str {
                    command.push(' ');
                    command.push_str(&columns_str);
                }
            }
        }
        command.push('\n');
        assert!(command.len() == capacity);

        let (nb_lines, _message) = self.send_command(&command).await?;
        let lines = self.read_n_lines(nb_lines).await?;

        let response = FetchResponse::from_lines(lines)?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use crate::now::now_timestamp;

    use super::*;
    use serial_test::serial;

    #[tokio::test]
    async fn test_ping_tcp() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();
        client.ping().await.unwrap();
    }

    #[tokio::test]
    async fn test_ping_unix() {
        let mut client = RRDCachedClient::connect_unix("./rrdcached.sock")
            .await
            .unwrap();
        client.ping().await.unwrap();
    }

    #[tokio::test]
    async fn test_help() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        let (header, lines) = client.help(None).await.unwrap();
        assert_eq!(header, "Command overview");
        assert_eq!(lines.len(), 22);

        let (header, lines) = client.help(Some("PING")).await.unwrap();
        assert_eq!(header, "Help for PING");
        assert!(!lines.is_empty());
    }

    async fn create_simple_rrd(client: &mut RRDCachedClient<TcpStream>, name: String) {
        client
            .create(CreateArguments {
                path: name,
                data_sources: vec![CreateDataSource {
                    name: "ds1".to_string(),
                    minimum: None,
                    maximum: None,
                    heartbeat: 10,
                    serie_type: CreateDataSourceType::Gauge,
                }],
                round_robin_archives: vec![CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 1,
                    rows: 100,
                }],
                start_timestamp: 1609459200,
                step_seconds: 1,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        client
            .create(CreateArguments {
                path: "test-create".to_string(),
                data_sources: vec![
                    CreateDataSource {
                        name: "ds1".to_string(),
                        minimum: None,
                        maximum: None,
                        heartbeat: 10,
                        serie_type: CreateDataSourceType::Gauge,
                    },
                    CreateDataSource {
                        name: "ds2".to_string(),
                        minimum: Some(0.0),
                        maximum: Some(100.0),
                        heartbeat: 10,
                        serie_type: CreateDataSourceType::Gauge,
                    },
                ],
                round_robin_archives: vec![
                    CreateRoundRobinArchive {
                        consolidation_function: ConsolidationFunction::Average,
                        xfiles_factor: 0.5,
                        steps: 1,
                        rows: 10,
                    },
                    CreateRoundRobinArchive {
                        consolidation_function: ConsolidationFunction::Average,
                        xfiles_factor: 0.5,
                        steps: 10,
                        rows: 10,
                    },
                ],
                start_timestamp: 1609459200,
                step_seconds: 1,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_update() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-update".to_string()).await;

        client.update_one("test-update", None, 4.2).await.unwrap();
    }

    #[tokio::test]
    async fn test_error() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        let result = client.list(false, Some("not-found-path")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_flush() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-flush".to_string()).await;

        client.update_one("test-flush", None, 4.2).await.unwrap();
        client.flush("test-flush").await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_all() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        client.flush_all().await.unwrap();
    }

    #[serial]
    #[tokio::test]
    async fn test_pending() {
        // Wait 0.1s
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-pending".to_string()).await;

        client.update_one("test-pending", None, 4.2).await.unwrap();

        let lines = client.pending("test-pending").await.unwrap();
        assert_eq!(lines.len(), 1);

        // Flush
        client.flush("test-pending").await.unwrap();
        // Wait 0.1s
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let lines = client.pending("test-pending").await.unwrap();
        assert!(lines.is_empty());
    }

    #[tokio::test]
    async fn test_forget() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-forget".to_string()).await;

        client.update_one("test-forget", None, 4.2).await.unwrap();
        client.forget("test-forget").await.unwrap();
    }

    #[tokio::test]
    async fn test_queue() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        let lines = client.queue().await.unwrap();

        // I didn't manage to get a non-empty queue...
        assert!(lines.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        let stats = client.stats().await.unwrap();
        assert!(!stats.is_empty());
    }

    #[tokio::test]
    async fn test_first_and_last() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-first".to_string()).await;

        // This can fail for subsequent runs
        let _ = client.update_one("test-first", Some(1612345678), 4.2).await;

        let timestamp = client.first("test-first", None).await.unwrap();
        assert_eq!(timestamp, 1609459101); // I'm guessing some alignment is happening

        let timestamp = client.last("test-first").await.unwrap();
        assert_eq!(timestamp, 1612345678);
    }

    #[tokio::test]
    async fn test_info() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-info".to_string()).await;
        let lines = client.info("test-info").await.unwrap();
        assert!(!lines.is_empty());
    }

    #[tokio::test]
    async fn test_list() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        let lines = client.list(true, None).await.unwrap();
        assert!(!lines.is_empty());
    }

    #[serial]
    #[tokio::test]
    async fn test_suspend_and_resume() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-suspend".to_string()).await;

        // insert some data and flush
        client.update_one("test-suspend", None, 4.2).await.unwrap();
        client.flush("test-suspend").await.unwrap();

        client.suspend("test-suspend").await.unwrap();
        client.resume("test-suspend").await.unwrap();
    }

    #[serial]
    #[tokio::test]
    async fn test_suspend_all_and_resume_all() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        client.suspend_all().await.unwrap();
        client.resume_all().await.unwrap();
    }

    #[tokio::test]
    async fn test_quit() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        client.quit().await.unwrap();

        // check that the connection is closed
        let result = client.ping().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        create_simple_rrd(&mut client, "test-batch-1".to_string()).await;
        create_simple_rrd(&mut client, "test-batch-2".to_string()).await;

        let commands = vec![
            BatchUpdate::new("test-batch-1", None, vec![1.0]).unwrap(),
            BatchUpdate::new("test-batch-2", None, vec![2.0]).unwrap(),
        ];
        client.batch(commands).await.unwrap();

        // Let's do the errors, it will fail
        // because the time is the same
        let commands = vec![
            BatchUpdate::new("test-batch-1", None, vec![3.0]).unwrap(),
            BatchUpdate::new("test-batch-2", None, vec![4.0]).unwrap(),
        ];
        let result = client.batch(commands).await;
        assert!(result.is_err());
    }

    #[serial]
    #[tokio::test]
    async fn test_fetch() {
        let mut client = RRDCachedClient::connect_tcp("localhost:42217")
            .await
            .unwrap();

        client
            .create(CreateArguments {
                path: "test-fetch".to_string(),
                data_sources: vec![
                    CreateDataSource {
                        name: "ds1".to_string(),
                        minimum: None,
                        maximum: None,
                        heartbeat: 10,
                        serie_type: CreateDataSourceType::Gauge,
                    },
                    CreateDataSource {
                        name: "ds2".to_string(),
                        minimum: Some(0.0),
                        maximum: Some(100.0),
                        heartbeat: 10,
                        serie_type: CreateDataSourceType::Gauge,
                    },
                ],
                round_robin_archives: vec![
                    CreateRoundRobinArchive {
                        consolidation_function: ConsolidationFunction::Average,
                        xfiles_factor: 0.5,
                        steps: 1,
                        rows: 10,
                    },
                    CreateRoundRobinArchive {
                        consolidation_function: ConsolidationFunction::Average,
                        xfiles_factor: 0.5,
                        steps: 10,
                        rows: 10,
                    },
                ],
                start_timestamp: 1609459200,
                step_seconds: 1,
            })
            .await
            .unwrap();

        let result = client
            .fetch(
                "test-fetch",
                ConsolidationFunction::Average,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.flush_version, 1);
        assert!(result.start > 0);
        assert!(result.end > 0);
        assert_eq!(result.step, 1);
        assert_eq!(result.ds_count, 2);
        assert_eq!(result.ds_names, vec!["ds1".to_string(), "ds2".to_string()]);
        assert!(!result.data.is_empty());

        // Test the errors in parameters
        let result = client
            .fetch(
                "test-fetch",
                ConsolidationFunction::Average,
                None,
                Some(1609459200),
                None,
            )
            .await;
        assert!(result.is_err());

        let result = client
            .fetch(
                "test-fetch",
                ConsolidationFunction::Average,
                Some(1609459200),
                None,
                Some(vec!["ds1".to_string(), "ds2".to_string()]),
            )
            .await;
        assert!(result.is_err());

        let now_timestamp = now_timestamp().unwrap();
        let result = client
            .fetch(
                "test-fetch",
                ConsolidationFunction::Average,
                Some(now_timestamp as i64 - 10),
                Some(now_timestamp as i64),
                Some(vec!["not-found".to_string()]),
            )
            .await;

        assert!(result.is_err());
        let result = client
            .fetch(
                "test-fetch",
                ConsolidationFunction::Average,
                Some(now_timestamp as i64 - 10),
                Some(now_timestamp as i64),
                Some(vec!["ds2".to_string()]),
            )
            .await
            .unwrap();
        assert_eq!(result.ds_count, 1);

        // Relative timestamp
        let result = client
            .fetch(
                "test-fetch",
                ConsolidationFunction::Average,
                Some(-10),
                Some(0),
                Some(vec!["ds2".to_string()]),
            )
            .await
            .unwrap();
        assert_eq!(result.ds_count, 1);
    }
}
