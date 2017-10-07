#![feature(proc_macro, conservative_impl_trait, generators, vec_resize_default, integer_atomics)]

extern crate bytes;
extern crate clap;
extern crate futures_await as futures;
extern crate mqtt;
extern crate native_tls;
extern crate num_cpus;
extern crate rustls;
extern crate time;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_rustls;
extern crate tokio_service;
extern crate tokio_timer;
extern crate tokio_tls;

use futures::prelude::*;
use std::net::SocketAddr;
use mqtt::{Codec, Connect, ConnectReturnCode, LastWill, Packet, Protocol, QoS};
use tokio_proto::pipeline::{ClientProto, ClientService};
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use std::{cmp, io, thread};
use futures::{future, Future, Sink, Stream};
use tokio_service::Service;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

static PAYLOAD_SOURCE: &[u8] = include_bytes!("lorem.txt");

#[async]
fn tokio_delay(val: Duration, loop_handle: Handle) -> Result<(), io::Error> {
    await!(tokio_core::reactor::Timeout::new(val, &loop_handle)?)?;
    Ok(())
}

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
                -d, --delay=[MILLISECONDS] 'delay in milliseconds between two calls made for the same connection'
                --connection-rate=[COUNT] 'number of connections allowed to open concurrently (per thread)'
                -t, --threads=[NUMBER] 'number of threads to use'",
        )
        .get_matches();

    let addr: SocketAddr = matches.value_of("address").unwrap().parse().unwrap();
    let payload_size: usize = parse_u64_default(matches.value_of("size"), 0) as usize * 1024;
    let concurrency = parse_u64_default(matches.value_of("concurrency"), 1);
    let threads = cmp::min(
        concurrency,
        parse_u64_default(matches.value_of("threads"), num_cpus::get() as u64),
    );
    let warmup_seconds = parse_u64_default(matches.value_of("warm-up"), 2) as u64;
    let sample_rate = parse_u64_default(matches.value_of("sample-rate"), 1) as u64;
    let delay = Duration::from_millis(parse_u64_default(matches.value_of("delay"), 0));

    let connections_per_thread = cmp::max(concurrency / threads, 1);
    let connection_rate = parse_u64_default(matches.value_of("connection-rate"), connections_per_thread) as usize;
    let perf_counters = Arc::new(PerfCounters::new());
    let threads = (0..threads)
        .map(|i| {
            let counters = perf_counters.clone();
            thread::Builder::new()
                .name(format!("worker{}", i))
                .spawn(move || {
                    push(
                        addr,
                        connections_per_thread as usize,
                        (i * connections_per_thread) as usize,
                        connection_rate,
                        payload_size,
                        delay,
                        &counters,
                    )
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let counters = perf_counters.clone();
    let monitor_thread = thread::Builder::new()
        .name("monitor".to_string())
        .spawn(move || {
            thread::sleep(Duration::from_secs(warmup_seconds));
            println!("warm up past");
            let mut prev_reqs = 0;
            let mut prev_lat = 0;
            loop {
                let reqs = counters.request_count();
                if reqs > prev_reqs {
                    let latency = counters.latency_ns();
                    let req_count = (reqs - prev_reqs) as u64;
                    let latency_diff = latency - prev_lat;
                    println!(
                        "rate: {}, latency: {}",
                        req_count / sample_rate,
                        time::Duration::nanoseconds((latency_diff / req_count) as i64)
                    );
                    prev_reqs = reqs;
                    prev_lat = latency;
                }
                thread::sleep(Duration::from_secs(sample_rate));
            }
        })
        .unwrap();

    for thread in threads {
        thread.join().unwrap();
    }
    monitor_thread.join().unwrap();
}

fn parse_u64_default(input: Option<&str>, default: u64) -> u64 {
    input
        .map(|v| v.parse().expect(&format!("not a valid number: {}", v)))
        .unwrap_or(default)
}

fn push(addr: SocketAddr, connections: usize, offset: usize, rate: usize, payload_size: usize, delay: Duration, perf_counters: &Arc<PerfCounters>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let payload = Bytes::from_static(&PAYLOAD_SOURCE[..payload_size]);

    let timestamp = time::precise_time_ns();
    let conn_stream = futures::stream::iter_ok(0..connections)
        .map(|i| {
            Client::connect(addr, format!("client_{}", offset + i), handle.clone())
        })
        .buffered(rate)
        .collect()
        .and_then(|connections| {
            println!(
                "done connecting in {}",
                time::Duration::nanoseconds((time::precise_time_ns() - timestamp) as i64)
            );
            future::join_all(connections.into_iter().map(|conn| {
                conn.run(payload.slice_from(0), delay, perf_counters.clone())
            }))
        })
        .and_then(|_| Ok(()))
        .map_err(|e| {
            println!("error: {:?}", e);
            e
        });
    core.run(conn_stream).unwrap();
}

pub struct Client {
    io: ClientIo,
    loop_handle: Handle,
}

enum ClientIo {
    Direct(ClientService<TcpStream, MqttProto>),
    Secured(ClientService<TcpStream, tokio_tls::proto::Client<MqttProto>>),
}

impl Client {
    #[async]
    pub fn connect(addr: SocketAddr, client_id: String, handle: Handle) -> Result<Client, io::Error> {
        #[async]
        for _ in futures::stream::repeat::<_, io::Error>(0) {
            match await!(Client::connect_internal(addr, client_id.clone(), handle.clone())) {
                Ok(c) => {
                    return Ok(c);
                }
                Err(e) => {
                    print!("!"); // todo: log e?
                    await!(tokio_delay(Duration::from_secs(2), handle.clone()))?;
                }
            }
        }
        Err(io::Error::from(io::ErrorKind::NotConnected))
    }

    #[async]
    fn connect_internal(addr: SocketAddr, client_id: String, handle: Handle) -> Result<Client, io::Error> {
        if addr.port() == 8883 {
            let connector = native_tls::TlsConnector::builder()
                .unwrap()
                .build()
                .unwrap();
            let tls_client = tokio_tls::proto::Client::new(MqttProto { client_id }, connector, "dotnetty.com");
            let service = await!(tokio_proto::TcpClient::new(tls_client).connect(&addr, &handle))?;
            Ok(Client {
                io: ClientIo::Secured(service),
                loop_handle: handle,
            })
        } else {
            let service = await!(tokio_proto::TcpClient::new(MqttProto { client_id }).connect(&addr, &handle))?;
            Ok(Client {
                io: ClientIo::Direct(service),
                loop_handle: handle,
            })
        }
    }

    fn call(&self, req: Packet) -> impl Future<Item = Packet, Error = io::Error> {
        match self.io {
            ClientIo::Direct(ref inner) => future::Either::A(inner.call(req)),
            ClientIo::Secured(ref inner) => future::Either::B(inner.call(req)),
        }
    }

    #[async]
    pub fn run(self, payload: Bytes, delay: Duration, perf_counters: Arc<PerfCounters>) -> Result<(), io::Error> {
        let perf_counters = perf_counters.clone();
        #[async]
        for _ in futures::stream::repeat::<_, io::Error>(0) {
            let timestamp = time::precise_time_ns();
            let response = await!(self.call(Packet::Publish {
                qos: QoS::AtLeastOnce,
                packet_id: Some(1000),
                payload: payload.slice_from(0),
                topic: "$iothub/twin/PATCH/properties/reported/?version=1ac5".to_string(),
                dup: false,
                retain: false,
            }))?;
            match response {
                Packet::PublishAck { .. } => {
                    perf_counters.register_request();
                    perf_counters.register_latency(time::precise_time_ns() - timestamp);
                }
                _ => {
                    return Err(io::Error::new(io::ErrorKind::Other, "unexpected response"));
                }
            }
            await!(tokio_delay(delay, self.loop_handle.clone()))?;
        }
        Ok(())
    }
}

pub struct PerfCounters {
    req: AtomicUsize,
    lat: AtomicU64,
}

impl PerfCounters {
    pub fn new() -> PerfCounters {
        PerfCounters {
            req: AtomicUsize::new(0),
            lat: AtomicU64::new(0),
        }
    }

    pub fn request_count(&self) -> usize {
        self.req.load(Ordering::SeqCst)
    }

    pub fn latency_ns(&self) -> u64 {
        self.lat.load(Ordering::SeqCst)
    }

    pub fn register_request(&self) {
        self.req.fetch_add(1, Ordering::SeqCst);
    }

    pub fn register_latency(&self, nanos: u64) {
        self.lat.fetch_add(nanos, Ordering::SeqCst);
    }
}

pub struct MqttProto { client_id: String }

impl<T: tokio_io::AsyncRead + tokio_io::AsyncWrite + Send + 'static> ClientProto<T> for MqttProto {
    type Request = Packet;
    type Response = Packet;
    type Transport = Framed<T, Codec>;
    type BindTransport = Box<Future<Item = Self::Transport, Error = io::Error>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        MqttProto::bind_transport_inner(self.client_id.clone(), io)
    }
}

impl MqttProto {
    #[async(boxed)]
    fn bind_transport_inner<T: tokio_io::AsyncRead + tokio_io::AsyncWrite + Send + 'static>(client_id: String, io: T) -> Result<Framed<T, Codec>, io::Error> {
        let transport: Framed<T, Codec> = io.framed(Codec);

        let transport = await!(transport.send(Packet::Connect {
            connect: Box::new(Connect {
                protocol: Protocol::MQTT(4), // todo
                client_id: client_id,
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
        }))?;
        let (packet, transport) = await!(transport.into_future().map_err(|(e, _)| e))?;
        match packet {
            Some(Packet::ConnectAck { return_code, .. }) if return_code == ConnectReturnCode::ConnectionAccepted => Ok(transport),
            Some(Packet::ConnectAck { .. }) => Err(io::Error::new(
                io::ErrorKind::Other,
                "CONNECT was not accepted",
            )),
            _ => Err(io::Error::new(io::ErrorKind::Other, "protocol violation")),
        }
    }
}