#![feature(proc_macro, generators, vec_resize_default, integer_atomics)]

extern crate bytes;
extern crate clap;
extern crate futures_await as futures;
extern crate mqtt;
extern crate string;
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
use mqtt::{Connection, QoS, Error, ErrorKind};
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use std::{cmp, io, thread};
use futures::{future, Future, Stream};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use native_tls::TlsConnector;
use tokio_tls::TlsConnectorExt;

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
                    let latency_max = counters.pull_latency_max_ns();
                    let req_count = (reqs - prev_reqs) as u64;
                    let latency_diff = latency - prev_lat;
                    println!(
                        "rate: {}, latency: {}, latency max: {}",
                        req_count / sample_rate,
                        time::Duration::nanoseconds((latency_diff / req_count) as i64),
                        time::Duration::nanoseconds(latency_max as i64)
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
    println!("now connecting at rate {}", rate);
    let conn_stream = futures::stream::iter_ok(0..connections)
        .map(|i| Client::connect(addr, format!("client_{}", offset + i), handle.clone()))
        .buffered(rate)
        .collect()
        .and_then(|connections| {
            println!(
                "done connecting {} in {}",
                connections.len(),
                time::Duration::nanoseconds((time::precise_time_ns() - timestamp) as i64)
            );
            future::join_all(connections.into_iter().map(|conn| {
                conn.run(payload.clone(), delay, perf_counters.clone())
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
    connection: Connection,
    loop_handle: Handle,
    client_id: String
}

impl Client {
    #[async]
    pub fn connect(addr: SocketAddr, client_id: String, handle: Handle) -> Result<Client, Error> {
        #[async]
        for _ in futures::stream::repeat::<_, io::Error>(0) {
            match await!(Client::connect_internal(addr, client_id.clone(), handle.clone())) {
                Ok(c) => {
                    return Ok(c);
                }
                Err(_e) => {
                    print!("!"); // todo: log e?
                    await!(tokio_delay(Duration::from_secs(20), handle.clone()))?;
                }
            }
        }
        Err(io::Error::from(io::ErrorKind::NotConnected).into())
    }

    #[async]
    fn connect_internal(addr: SocketAddr, client_id: String, handle: Handle) -> Result<Client, Error> {
        let socket = await!(TcpStream::connect(&addr, &handle.clone()))?;
        if addr.port() == 8883 {
            let tls_context = TlsConnector::builder()
                .unwrap()
                .build()
                .unwrap();
            let io = await!(tls_context.connect_async("gateway.tests.com", socket).map_err(|e| {
                Error::with_chain(e, ErrorKind::Msg("TLS handshake failed".into()))
            }))?;
            
            let connection = await!(Connection::open(bytes_to_string(client_id.clone().into()), handle.clone(), io))?;
            Ok(Client {
                connection,
                loop_handle: handle,
                client_id: client_id
            })
        } else {
            let connection = await!(Connection::open(bytes_to_string(client_id.clone().into()), handle.clone(), socket))?;
            Ok(Client {
                connection,
                loop_handle: handle,
                client_id: client_id
            })
        }
    }

    #[async]
    pub fn run(self, payload: Bytes, delay: Duration, perf_counters: Arc<PerfCounters>) -> Result<(), Error> {
        let perf_counters = perf_counters.clone();
        #[async]
        for _ in futures::stream::repeat::<_, io::Error>(0) {
            if delay > Duration::default() {
                await!(tokio_delay(delay, self.loop_handle.clone()))?;
            }
            let timestamp = time::precise_time_ns();
            await!(self.connection.send(
                QoS::AtLeastOnce,
                bytes_to_string(format!("/devices/{}/messages/events/", self.client_id).into()),
                payload.clone()))?;
            perf_counters.register_request();
            perf_counters.register_latency(time::precise_time_ns() - timestamp);
        }
        Ok(())
    }
}

pub struct PerfCounters {
    req: AtomicUsize,
    lat: AtomicU64,
    lat_max: AtomicU64
}

impl PerfCounters {
    pub fn new() -> PerfCounters {
        PerfCounters {
            req: AtomicUsize::new(0),
            lat: AtomicU64::new(0),
            lat_max: AtomicU64::new(0),
        }
    }

    pub fn request_count(&self) -> usize {
        self.req.load(Ordering::SeqCst)
    }

    pub fn latency_ns(&self) -> u64 {
        self.lat.load(Ordering::SeqCst)
    }

    pub fn pull_latency_max_ns(&self) -> u64 {
        self.lat_max.swap(0, Ordering::SeqCst)
    }

    pub fn register_request(&self) {
        self.req.fetch_add(1, Ordering::SeqCst);
    }

    pub fn register_latency(&self, nanos: u64) {
        self.lat.fetch_add(nanos, Ordering::SeqCst);
        loop {
            let current = self.lat_max.load(Ordering::SeqCst);
            if current >= nanos || self.lat_max.compare_and_swap(current, nanos, Ordering::SeqCst) == current {
                break;
            }
        }
    }
}

fn bytes_to_string(b: Bytes) -> string::String<Bytes> {
    unsafe { string::String::from_utf8_unchecked(b) }
}
