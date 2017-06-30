#![feature(conservative_impl_trait)]

extern crate tokio_io as tokio_io;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate clap;
extern crate num_cpus;
extern crate mqtt;
extern crate bytes;
extern crate futures;

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
use std::sync::atomic::{AtomicUsize, Ordering};

static PAYLOAD_SOURCE: &[u8] = include_bytes!("lorem.txt");

fn main() {
    let matches = clap::App::new("MQTT Push")
        .version("0.1")
        .about("Applies load to MQTT broker")
        .args_from_usage(
            "<address> 'IP address and port to push'
                              -s, --size=[NUMBER] 'size of PUBLISH packet payload to send'
                              -c, --concurrency=[NUMBER] 'number of MQTT connections to open and use concurrently for sending'
                              -t, --threads=[NUMBER] 'number of threads to use'",
        )
        .get_matches();

    let addr: SocketAddr = matches.value_of("address").unwrap().parse().unwrap();
    let payload_size: usize = matches
        .value_of("size")
        .map(|v| v.parse().unwrap())
        .unwrap_or(0) * 1024;
    let concurrency: usize = matches
        .value_of("concurrency")
        .map(|v| v.parse().unwrap())
        .unwrap_or(1);
    let threads: usize = cmp::min(
        concurrency,
        matches
            .value_of("threads")
            .map(|v| v.parse().unwrap())
            .unwrap_or(num_cpus::get()),
    );

    let connections_per_thread = cmp::max(concurrency / threads, 1);
    let req_counter = Arc::new(AtomicUsize::new(0));
    let threads = (0..threads)
        .map(|i| {
            let requests = req_counter.clone();
            thread::Builder::new()
                .name(format!("worker{}", i))
                .spawn(move || push(addr, connections_per_thread, payload_size, &requests))
                .unwrap()
        })
        .collect::<Vec<_>>();
    
    let requests = req_counter.clone();
    let monitor_thread = thread::Builder::new()
        .name("monitor".to_string())
        .spawn(move || {
            let mut prev_reqs = 0;
            loop {
                let reqs = requests.load(Ordering::SeqCst);
                if reqs > prev_reqs {
                    println!("rate: {}", (reqs - prev_reqs) / 2);
                    prev_reqs = reqs;
                }
                thread::sleep(std::time::Duration::from_secs(2));
            }
        })
        .unwrap();

    for thread in threads {
        thread.join().unwrap();
    }
    monitor_thread.join().unwrap();
}

fn push(addr: SocketAddr, connections: usize, payload_size: usize, req_counter: &Arc<AtomicUsize>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let payload = Bytes::from(&PAYLOAD_SOURCE[..payload_size]);

    let connections = future::join_all((0..connections)
        .map(move |_| Client::connect(&addr, &handle)))
        .and_then(|connections| {
            println!("done connecting");
            future::join_all(connections.into_iter().map(|conn| conn.run(&payload, req_counter)))
        })
        .and_then(|_| Ok(()))
        .map_err(|e| {
            println!("error: {:?}", e);
            e
        });
    core.run(connections).unwrap();
}

pub struct Client {
    inner: ClientService<TcpStream, MqttProto>,
}

impl Client {
    pub fn connect(addr: &SocketAddr, handle: &Handle) -> impl Future<Item = Client, Error = io::Error> {
        tokio_proto::TcpClient::new(MqttProto)
            .connect(addr, handle)
            .map(|service| Client {inner: service})
    }

    pub fn run(self, payload: &Bytes, req_counter: &Arc<AtomicUsize>) -> Box<Future<Item=(), Error=io::Error>> {
        let req_counter = req_counter.clone();
        Box::new(loop_fn((self, payload.slice_from(0), req_counter), |(client, payload, req_counter)| {
            client.inner.call(Packet::Publish {
                qos: QoS::AtLeastOnce,
                packet_id: Some(1000),
                payload: payload.slice_from(0),
                topic: "$iothub/twin/PATCH/properties/reported/?version=1ac5".to_string(),
                dup: false,
                retain: false,
            })
            .and_then(|response| {
                match response {
                    Packet::PublishAck { .. } => {
                        req_counter.fetch_add(1, Ordering::SeqCst);
                        Ok(Loop::Continue((client, payload, req_counter)))
                    },
                    _ => Err(io::Error::new(io::ErrorKind::Other, "unexpected response"))
                }
            })
        }))
    }
}

struct MqttProto;

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
