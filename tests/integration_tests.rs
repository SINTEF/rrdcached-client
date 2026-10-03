use rrdcached_client::{
    RRDCachedClient,
    batch_update::BatchUpdate,
    consolidation_function::ConsolidationFunction,
    create::{CreateArguments, CreateDataSource, CreateDataSourceType, CreateRoundRobinArchive},
    now::now_timestamp,
};
use tokio::net::TcpStream;

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
async fn test_create() {
    let mut client = RRDCachedClient::connect_tcp("localhost:42217")
        .await
        .unwrap();

    client
        .create(CreateArguments {
            path: "integration-test-create".to_string(),
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

/// A unique RRD name, so that the tests can be run again without waiting
/// for the previous run's updates to be in the past.
fn unique_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
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
async fn test_update() {
    let mut client = RRDCachedClient::connect_tcp("localhost:42217")
        .await
        .unwrap();

    let name = unique_name("test-integrations-update");
    create_simple_rrd(&mut client, name.clone()).await;
    client.update_one(&name, None, 4.2).await.unwrap();
}

#[tokio::test]
async fn test_double_create() {
    let mut client = RRDCachedClient::connect_tcp("localhost:42217")
        .await
        .unwrap();

    let name = unique_name("test-integrations-double-create");
    create_simple_rrd(&mut client, name.clone()).await;
    let timestamp_last = client.last(&name).await.unwrap();
    client.update_one(&name, None, 4.2).await.unwrap();
    let new_timestamp = client.last(&name).await.unwrap();

    assert!(new_timestamp > timestamp_last);

    create_simple_rrd(&mut client, name.clone()).await;
    let not_overwritten_timestamp = client.last(&name).await.unwrap();
    assert_eq!(not_overwritten_timestamp, new_timestamp);
}

#[tokio::test]
async fn test_batch() {
    let mut client = RRDCachedClient::connect_tcp("localhost:42217")
        .await
        .unwrap();

    let name = unique_name("test-integrations-batch");
    create_simple_rrd(&mut client, name.clone()).await;

    let now = now_timestamp().unwrap();
    let commands = vec![
        BatchUpdate::new(&name, Some(now - 2), vec![1.0]).unwrap(),
        BatchUpdate::new(&name, None, vec![2.0]).unwrap(),
    ];
    client.batch(commands).await.unwrap();
}
