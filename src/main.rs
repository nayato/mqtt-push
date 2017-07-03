#![feature(conservative_impl_trait, integer_atomics)]

extern crate tokio_io;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate native_tls;
extern crate tokio_tls;
extern crate clap;
extern crate num_cpus;
extern crate mqtt;
extern crate bytes;
extern crate time;
extern crate futures;
extern crate rustls;
extern crate tokio_rustls;
extern crate tokio_timer;

use std::net::SocketAddr;
use mqtt::{Packet, Connect, Protocol, QoS, LastWill, ConnectReturnCode, Codec};
use tokio_proto::pipeline::{ClientProto, ClientService};
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use std::{io, cmp, thread};
use futures::{future, Future, Sink, Stream};
use futures::future::{loop_fn, Loop};
use tokio_service::Service;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use time::{Duration, precise_time_ns};

static PAYLOAD_SOURCE: &[u8] = include_bytes!("lorem.txt");

fn main() {
    let matches = clap::App::new("MQTT Push")
        .version("0.1")
        .about("Applies load to MQTT broker")
        .args_from_usage(
            "<address> 'IP address and port to push'
                              -s, --size=[NUMBER] 'size of PUBLISH packet payload to send'
                              -c, --concurrency=[NUMBER] 'number of MQTT connections to open and use concurrently for sending'
                              -w, --warm-up=[SECONDS] 'seconds before counter values are considered for reporting'
                              -r, --sample-rate=[SECONDS] 'seconds between average reports'
                              -d, --delay=[MILLISECONDS] 'delay in milliseconds between two calls are made for the same connection'
                              -t, --threads=[NUMBER] 'number of threads to use'",
        )
        .get_matches();

    let addr: SocketAddr = matches.value_of("address").unwrap().parse().unwrap();
    let payload_size: usize = parse_u32_default(matches.value_of("size"), 0) as usize * 1024;
    let concurrency: usize = parse_u32_default(matches.value_of("concurrency"), 1) as usize;
    let threads: usize = cmp::min(
        concurrency,
        parse_u32_default(matches.value_of("threads"), num_cpus::get() as u32) as usize,
    );
    let warmup_seconds = parse_u32_default(matches.value_of("warm-up"), 2) as u64;
    let sample_rate = parse_u32_default(matches.value_of("sample-rate"), 1) as u64;

    let connections_per_thread = cmp::max(concurrency / threads, 1);
    let perf_counters = Arc::new(PerfCounters::new());
    let threads = (0..threads)
        .map(|i| {
            let counters = perf_counters.clone();
            thread::Builder::new()
                .name(format!("worker{}", i))
                .spawn(move || push(addr, connections_per_thread, payload_size, &counters))
                .unwrap()
        })
        .collect::<Vec<_>>();
    
    let counters = perf_counters.clone();
    let monitor_thread = thread::Builder::new()
        .name("monitor".to_string())
        .spawn(move || {
            thread::sleep(std::time::Duration::from_secs(warmup_seconds));
            let mut prev_reqs = 0;
            let mut prev_lat = Duration::zero();
            loop {
                let reqs = counters.req();
                if reqs > prev_reqs {
                    let lat = counters.lat();
                    let req_count = reqs - prev_reqs;
                    let sum_lat = (lat - prev_lat).num_nanoseconds().unwrap();
                    println!("rate: {}, latency: {}", (req_count) / 2, Duration::nanoseconds(sum_lat / (req_count as i64)));
                    prev_reqs = reqs;
                    prev_lat = lat;
                }
                thread::sleep(std::time::Duration::from_secs(sample_rate));
            }
        })
        .unwrap();

    for thread in threads {
        thread.join().unwrap();
    }
    monitor_thread.join().unwrap();
}

fn parse_u32_default(input: Option<&str>, default: u32) -> u32 {
    input
        .map(|v| v.parse().expect(&format!("not a valid number: {}", v)))
        .unwrap_or(default)
}

fn push(addr: SocketAddr, connections: usize, payload_size: usize, perf_counters: &Arc<PerfCounters>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let payload = Bytes::from_static(&PAYLOAD_SOURCE[..payload_size]);

    let connections = future::join_all((0..connections)
        .map(move |_| Client::connect(&addr, &handle)))
        .and_then(|connections| {
            println!("done connecting");
            future::join_all(connections.into_iter().map(|conn| conn.run(&payload, perf_counters)))
        })
        .and_then(|_| Ok(()))
        .map_err(|e| {
            println!("error: {:?}", e);
            e
        });
    core.run(connections).unwrap();
}

pub enum Client {
    Direct{inner: ClientService<TcpStream, MqttProto>},
    Secured{inner: ClientService<TcpStream, tokio_tls::proto::Client<MqttProto>>}
}

impl Client {
    pub fn connect(addr: &SocketAddr, handle: &Handle) -> impl Future<Item = Client, Error = io::Error> {
        if addr.port() == 8883 {
            let connector = native_tls::TlsConnector::builder().unwrap().build().unwrap();
            let tls_client = tokio_tls::proto::Client::new(MqttProto, connector, "dotnetty.com");
            future::Either::A(
                tokio_proto::TcpClient::new(tls_client)
                    .connect(addr, handle)
                    .map(|service| Client::Secured {inner: service}))
        }
        else {
            future::Either::B(
                tokio_proto::TcpClient::new(MqttProto)
                    .connect(addr, handle)
                    .map(|service| Client::Direct {inner: service}))
        }
    }

    fn call(&self, req: Packet) -> impl Future<Item = Packet, Error = io::Error> {
        match *self {
            Client::Direct{ref inner} => future::Either::A(inner.call(req)),
            Client::Secured{ref inner} => future::Either::B(inner.call(req))
        }
    }

    pub fn run(self, payload: &Bytes, perf_counters: &Arc<PerfCounters>) -> Box<Future<Item=(), Error=io::Error>> {
        let perf_counters = perf_counters.clone();
        Box::new(loop_fn((self, payload.slice_from(0), perf_counters), |(client, payload, perf_counters)| {
            let timestamp = precise_time_ns();
            client.call(Packet::Publish {
                qos: QoS::AtLeastOnce,
                packet_id: Some(1000),
                payload: payload.slice_from(0),
                topic: "$iothub/twin/PATCH/properties/reported/?version=1ac5".to_string(),
                dup: false,
                retain: false,
            })
            .and_then(move |response| {
                match response {
                    Packet::PublishAck { .. } => {
                        perf_counters.add_req();
                        perf_counters.add_lat_ns(precise_time_ns() - timestamp);
                        Ok(Loop::Continue((client, payload, perf_counters)))
                    },
                    _ => Err(io::Error::new(io::ErrorKind::Other, "unexpected response"))
                }
            })
        }))
    }
}

pub struct PerfCounters { req: AtomicUsize, lat: AtomicU64}

impl PerfCounters {
    pub fn new() -> PerfCounters {
        PerfCounters { req: AtomicUsize::new(0), lat: AtomicU64::new(0) } 
    }

    pub fn req(&self) -> usize {
        self.req.load(Ordering::SeqCst)
    }

    pub fn lat(&self) -> Duration {
        Duration::nanoseconds(self.lat.load(Ordering::SeqCst) as i64)
    }

    pub fn add_req(&self) {
        self.req.fetch_add(1, Ordering::SeqCst);
    }

    pub fn add_lat_ns(&self, val: u64) {
        self.lat.fetch_add(val, Ordering::SeqCst);
    }
}

pub struct MqttProto;

impl<T: tokio_io::AsyncRead + tokio_io::AsyncWrite + 'static> ClientProto<T> for MqttProto {
    type Request = Packet;
    type Response = Packet;
    type Transport = Framed<T, Codec>;
    type BindTransport = Box<Future<Item = Self::Transport, Error = io::Error>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        let transport: Framed<T, Codec> = io.framed(Codec);

        let handshake = transport.send(Packet::Connect {
            connect: Box::new(Connect {
                protocol: Protocol::MQTT(4), // todo
                client_id: "abc".to_owned(),
                clean_session: false,
                keep_alive: 300,
                username: Some("testuser".to_owned()),
                password: Some("notsafe".into()),
                last_will: Some(LastWill {
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "last/word".to_owned(),
                    message: "oops".into(),
                }),
            }),
        })
        // Wait for a response from the server, if the transport errors out,
        // we don't care about the transport handle anymore, just the error
        .and_then(|transport| transport.into_future().map_err(|(e, _)| e))
        .and_then(|(packet, transport)| {
            // The server sent back a CONNACK, check to see if it is the
            // expected handshake line.
            match packet {
                Some(Packet::ConnectAck {return_code, ..}) if return_code == ConnectReturnCode::ConnectionAccepted => Ok(transport),
                Some(Packet::ConnectAck {..}) => Err(io::Error::new(io::ErrorKind::Other, "CONNECT was not accepted")),
                _ => Err(io::Error::new(io::ErrorKind::Other, "protocol violation"))
            }
        });
        Box::new(handshake)
    }
}
