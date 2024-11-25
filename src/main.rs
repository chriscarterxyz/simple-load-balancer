use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, BufRead, BufReader};
use std::sync::{Arc};
use std::error::{Error};

use tokio::sync::{Mutex};
use tokio::time::{sleep, Duration};

use chrono::prelude::*;

const HEALTHCHECK_PERIOD_MILLIS: u64 = 1 * 60 * 1000;

// verbosity level
// 0: none
// 1: request line only
// 2: request line and headers
// 3: request line, headers, and body
const VERBOSE: u8 = 1;

fn strip(s: String) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

fn now() -> String {
    format!("{}", Utc::now().format("%Y-%m-%d %H:%M:%S"))
}


fn read_http(stream: &mut TcpStream) -> Vec<u8> {
    
    let mut reader = BufReader::new(stream);
    let mut buf: Vec<u8> = vec![];
    let mut line: Vec<u8> = vec![];
    let mut content_length: usize = 0;

    // read first header line, either request or response line
    match reader.read_until(b'\n', &mut line) {
        Ok(_n) => {
            if VERBOSE >= 1 {
                print!("{}", String::from_utf8(line.clone()).unwrap());
            }

            buf.append(&mut line);
        },
        Err(_e) => println!("could not read")
    }

    // read the header
    loop {
        match reader.read_until(b'\n', &mut line) {
            Ok(_n) => {
                let header_line = String::from_utf8(line.clone()).unwrap();
                if VERBOSE >= 2 {
                    print!("{}", header_line);
                }

                let header: Vec<&str> = header_line.split(": ").collect();
                match header[0] {
                    "Content-Length" => {
                        content_length = strip(header[1].to_string()).parse::<usize>().unwrap();
                    }
                    &_ => {}
                }

                buf.append(&mut line);

                if header_line == "\r\n" { break };
            },
            Err(_e) => println!("could not read")
        }

    }

    // read the body
    let mut body = vec![0; content_length];

    let _ = reader.read_exact(&mut body);

    if VERBOSE >= 3 {
        match String::from_utf8(body.clone()) {
            Ok(decoded) => {
                println!("{}", decoded);
            },
            Err(_e) => {
                println!("unable to decode body");
            }
        }
        
    }

    buf.append(&mut body);

    buf
}



async fn load_balance(incoming: &mut TcpStream, hosts: Arc<Mutex<Vec<Host>>>, host_index: Arc<Mutex<usize>>) {

    // read the request from the client
    let request: Vec<u8> = read_http(incoming); 

    // find the next healthy host
    let mut host_index_lock = host_index.lock().await;
    let mut hosts_lock = hosts.lock().await;

    // loop until traffic is successfully routed
    let mut failures: usize;
    loop {

        // find a healthy host to route to
        failures = 0;
        while failures < hosts_lock.len() {

            // increment over the available *hosts_lock, round-robin style
            *host_index_lock = (*host_index_lock + 1) % (*hosts_lock).len();

            // if the host is healthy, bail out1
            if hosts_lock[*host_index_lock].healthy {
                break;
            }
                
            if VERBOSE > 0 { 
                println!("{} lb [WARN] {} is unhealthy", now(), hosts_lock[*host_index_lock].url);
            }
            failures += 1;
        }

        if failures >= hosts_lock.len() { 
            break; 
        }

        // attempt to connect to the host
        match TcpStream::connect(&hosts_lock[*host_index_lock].url) {

            // if everything is ok, route traffic to the host and back to the client
            Ok(mut host_stream) => {
                let _ = host_stream.write_all(&request);
                let response: Vec<u8> = read_http(&mut host_stream);
                let _ = incoming.write_all(&response);
                break
            },

            // if connecting fails, mark the host as unhealthy and loop to find another one
            Err(_e) => {
                if VERBOSE > 0 { 
                    println!("{} lb [WARN] marking unhealthy: {}", now(), hosts_lock[*host_index_lock].url);
                }
                hosts_lock[*host_index_lock].healthy = false;
            }
        }
    }
    

    if failures >= hosts_lock.len() { 
        println!("{} lb [WARN] no available hosts", now());
    } else {
        println!("{} lb [INFO] {}", now(),  hosts_lock[*host_index_lock].url);
    }
}

struct Host {
    url: String,
    healthy: bool,
}

async fn healthy(host: &Host) -> bool {
    let response = reqwest::get(format!("http://{}", host.url)).await;


    match response {
        Ok(resp) => {
            return resp.status().is_success();
        },
        Err(_e) => {
            return false;
        }
    }
}

async fn check_health(hosts: &Arc<Mutex<Vec<Host>>>) {
            
    let mut hosts_lock = hosts.lock().await;

    for host in &mut *hosts_lock {
        host.healthy = healthy(&host).await;
        if VERBOSE > 0 {
                
            match host.healthy {
                true => println!("{} lb [INFO] {} is healthy", now(), host.url),
                false => println!("{} lb [WARN] {} is unhealthy", now(), host.url),
            }
        }
    }
                
}

async fn initialize_hosts(host_urls: Vec<&str>) -> Vec<Host> {
    let mut hosts: Vec<Host> = Vec::new();
    for host_url in host_urls {
        let host = Host {
            url: host_url.into(),
            healthy: false,
        };
        hosts.push(host)
    }
    hosts
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // load balancer url
    let endpoint = "127.0.0.1:9876";

    let hosts_urls = vec![
        "127.0.0.1:8080",
        "127.0.0.1:8081",
        "127.0.0.1:8082",
    ];

    // initialize hosts 
    let hosts = Arc::new(Mutex::new(initialize_hosts(hosts_urls).await));
    
    // initialize the health check list
    let hosts_checkhealth = hosts.clone();
    tokio::spawn( async move {

        loop {
            check_health(&hosts_checkhealth).await;

            sleep(Duration::from_millis(HEALTHCHECK_PERIOD_MILLIS)).await;
        }

    });


    // listen on the load balancer endpoint
    let listener = TcpListener::bind(&endpoint).unwrap();

    // index for host
    let host_index: Arc<Mutex<usize>> = Arc::new(Mutex::new(0 as usize));

    for incoming in listener.incoming() {
        match incoming {
            Ok(mut incoming_stream) => {

                let hosts_incoming = hosts.clone();
                let host_index_incoming = host_index.clone();

                tokio::spawn(async move { 
                    load_balance(&mut incoming_stream, hosts_incoming, host_index_incoming).await
                    }
                );

            },
            Err(_e) => {
                println!("connection failed");
            }
        }
    }

    Ok(())

}
