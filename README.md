# RRDCached-Client

RRDCached-Client is a Rust™ RRDCached client library.

It lets you connect to a [RRDCached](https://rrdtool.org/rrdtool/doc/rrdcached.en.html) API. RRDCached is a server to handle many [RRDTool](https://oss.oetiker.ch/rrdtool/) databases.

RRDtool databases are time series databases using a round-robin data structure. A RRDTool database file is always the same size and location on the filesystem. It never grows. Old data is discarded over time, but it is possible to downsample the data to keep a longer history. For example, it's possible to have a 1-second resolution for a day, a 1-minute resolution for a month, a 1-hour resolution for a year, and a 1-day resolution for 10 years. Querying the database is practically instantaneous.

While this technology fell out of fashion compared to the more modern time series databases, such as Prometheus or InfluxDB, it is still appreciated because it is simple and works well for some workloads.

## Example

```rust
let mut client = RRDCachedClient::connect_tcp("localhost:42217")
    .await?;

create_simple_rrd(&mut client, "hello".to_string()).await;

client.update_one("hello", None, 4.2).await?;
```

## Running a RRDCached server.

The repository includes a Dockerfile to quickly run an RRDCached server for testing and development purposes. It listens on localhost:42217 (tcp).

```bash
docker build . -f rrdcached.Dockerfile -t rrdcached
docker run -it --rm -p 127.0.0.1:42217:42217 --name rrdcached rrdcached
```

To test the unix socket feature, you can use `socat` to create a unix socket proxy to the tcp server.

```bash
socat UNIX-LISTEN:./rrdcached.sock,reuseaddr,fork TCP:localhost:42217
```

## Why?

Playing with the RRDCached API did sound fun while it was sadly raining on the snow outside (24th of February 2024).

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

Please note that RRDTool and RRDCached are licensed under the GPL v2.0. But we connect through a Unix or TCP socket, so licensing this project under the Apache License is valid. If RRDTool or RRDCached were licensed under the Affero GPL, this project would also have to be licensed under the Affero GPL.

## Should I use this in production?

As the license says, there is no warranty for this free software. It has been developed over a few hours and isn't used in production.

The unit and integration test coverage is relatively high, and the library is designed to be safe and robust, thanks to the great Rust ecosystem. But it's a young project, and it must have bugs.

You should probably use a more modern time series database in production. But you may be an RRDTool enthusiast and find this library useful.