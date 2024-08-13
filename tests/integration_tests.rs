use rrdcached_client::{
    batch_update::BatchUpdate,
    consolidation_function::ConsolidationFunction,
    create::{CreateArguments, CreateDataSource, CreateDataSourceType, CreateRoundRobinArchive},
    now::now_timestamp,
    RRDCachedClient,
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

    create_simple_rrd(&mut client, "test-integrations-update".to_string()).await;
    client
        .update_one("test-integrations-update", None, 4.2)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_double_create() {
    let mut client = RRDCachedClient::connect_tcp("localhost:42217")
        .await
        .unwrap();

    create_simple_rrd(&mut client, "test-integrations-double-create".to_string()).await;
    let timestamp_last = client
        .last("test-integrations-double-create")
        .await
        .unwrap();
    client
        .update_one("test-integrations-double-create", None, 4.2)
        .await
        .unwrap();
    let new_timestamp = client
        .last("test-integrations-double-create")
        .await
        .unwrap();

    assert!(new_timestamp > timestamp_last);

    create_simple_rrd(&mut client, "test-integrations-double-create".to_string()).await;
    let not_overwritten_timestamp = client
        .last("test-integrations-double-create")
        .await
        .unwrap();
    assert_eq!(not_overwritten_timestamp, new_timestamp);
}

#[tokio::test]
async fn test_batch() {
    let mut client = RRDCachedClient::connect_tcp("localhost:42217")
        .await
        .unwrap();

    create_simple_rrd(&mut client, "test-integrations-batch".to_string()).await;

    let now = now_timestamp().unwrap();
    let commands = vec![
        BatchUpdate::new("test-integrations-batch", Some(now - 2), vec![1.0]).unwrap(),
        BatchUpdate::new("test-integrations-batch", None, vec![2.0]).unwrap(),
    ];
    client.batch(commands).await.unwrap();
}
