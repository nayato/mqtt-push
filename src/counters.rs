use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}},
    time::Duration, thread
};
use time;

pub struct PerfCounters {
    req: AtomicUsize,
    lat: AtomicU64,
    lat_max: AtomicU64,
    req_current: AtomicUsize
}

pub struct RequestStat {
    timestamp: u64
}

impl PerfCounters {
    pub fn new() -> PerfCounters {
        PerfCounters {
            req: AtomicUsize::new(0),
            lat: AtomicU64::new(0),
            lat_max: AtomicU64::new(0),
            req_current: AtomicUsize::new(0)
        }
    }

    pub fn request_count(&self) -> usize {
        self.req.load(Ordering::SeqCst)
    }

    pub fn request_current(&self) -> usize {
        self.req_current.load(Ordering::SeqCst)
    }

    pub fn latency_ns(&self) -> u64 {
        self.lat.load(Ordering::SeqCst)
    }

    pub fn pull_latency_max_ns(&self) -> u64 {
        self.lat_max.swap(0, Ordering::SeqCst)
    }

    pub fn start_request(&self) -> RequestStat {
        self.req_current.fetch_add(1, Ordering::SeqCst);
        RequestStat { timestamp: time::precise_time_ns() }
    }

    pub fn stop_request(&self, req: RequestStat) {
        self.req.fetch_add(1, Ordering::SeqCst);
        self.req_current.fetch_sub(1, Ordering::SeqCst);
        let nanos = time::precise_time_ns() - req.timestamp;
        self.lat.fetch_add(nanos, Ordering::SeqCst);
        loop {
            let current = self.lat_max.load(Ordering::SeqCst);
            if current >= nanos || self.lat_max.compare_and_swap(current, nanos, Ordering::SeqCst) == current {
                break;
            }
        }
    }
}

pub fn setup_monitor(counters: Arc<PerfCounters>, warmup_seconds: u64, sample_rate: u64) -> thread::JoinHandle<()> {
    let monitor_thread = thread::Builder::new()
        .name("monitor".to_string())
        .spawn(move || {
            thread::sleep(Duration::from_secs(warmup_seconds));
            println!("warm up past");
            let mut prev_reqs = 0;
            let mut prev_lat = 0;
            loop {
                let reqs = counters.request_count();
                {//if reqs > prev_reqs {
                    let reqs_current = counters.request_current();
                    let latency = counters.latency_ns();
                    let latency_max = counters.pull_latency_max_ns();
                    let req_count = (reqs - prev_reqs) as u64;
                    let latency_diff = latency - prev_lat;
                    let mqtt_current = ::mqtt::SEND_IN_FLIGHT.load(Ordering::SeqCst);
                    println!(
                        "rate: {}, latency: {}, latency max: {}, in-flight: {}, mqtt: {}",
                        req_count / sample_rate,
                        time::Duration::nanoseconds((latency_diff / req_count) as i64),
                        time::Duration::nanoseconds(latency_max as i64),
                        reqs_current,
                        mqtt_current
                    );
                    prev_reqs = reqs;
                    prev_lat = latency;
                }
                thread::sleep(Duration::from_secs(sample_rate));
            }
        })
        .unwrap();
    monitor_thread
}
