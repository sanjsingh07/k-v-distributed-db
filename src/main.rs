use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{RwLock, mpsc},
    time::timeout,
};

use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::Local;

const MAX_MSG_SIZE: usize = 512;

#[derive(Debug, Clone)]
enum NodeRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone)]
struct NodeConfig {
    addr: String,
    peers: Vec<String>,
    role: NodeRole,
    leader_addr: String,
}

async fn handle_client(
    mut socket: TcpStream,
    connection_count: Arc<AtomicUsize>,
    db: Arc<RwLock<HashMap<String, (String, Option<Instant>)>>>,
    config: Arc<NodeConfig>,
) {
    let addr = socket.peer_addr().unwrap();

    let current = connection_count.fetch_add(1, Ordering::SeqCst) + 1;
    println!("Connected: {} | Active: {}", addr, current);

    let mut buffer = [0; 1024];

    loop {
        match socket.read(&mut buffer).await {
            Ok(0) => {
                let current = connection_count.fetch_sub(1, Ordering::SeqCst) - 1;
                println!("Disconnected: {} | Active: {}", addr, current);
                return;
            }
            Ok(n) => {
                if n > MAX_MSG_SIZE {
                    let err = "Message too large\n";
                    if let Err(e) = socket.write_all(err.as_bytes()).await {
                        println!("Write error: {}", e);
                        return;
                    }
                    println!("Rejected large message from {}", addr);
                    continue;
                }

                let received = String::from_utf8_lossy(&buffer[..n]).trim().to_string();
                let now = Local::now();
                let timestamp = now.format("%Y-%m-%d %H:%M:%S");

                println!("[{}][{}] Received: {}", timestamp, addr, received);

                let parts: Vec<&str> = received.split_whitespace().collect();

                if parts.is_empty() {
                    let err = "Empty command\n";
                    if let Err(e) = socket.write_all(err.as_bytes()).await {
                        println!("Write error: {}", e);
                        return;
                    }
                    println!("Received empty command from {}", addr);
                    continue;
                }

                let (command_parts, is_forwarded) = if parts[0] == "FORWARD" {
                    (&parts[1..], true)
                } else {
                    (&parts[..], false)
                };

                let command = command_parts[0].to_uppercase();

                let is_write = matches!(command.as_str(), "SET" | "DELETE" | "PERSIST");

                let key = command_parts.get(1).unwrap_or(&"").to_string();

                if matches!(config.role, NodeRole::Follower) && is_write && !is_forwarded {
                    let forward_req = format!("FORWARD {}\n", received);
                    let response = forward_request(&config.leader_addr, &forward_req).await;
                    socket.write_all(response.as_bytes()).await.unwrap();
                    continue;
                }

                let response = match command.as_str() {
                    "SET" => {
                        if command_parts.len() < 3 {
                            "Usage: SET key value\n".to_string()
                        } else {
                            let (value, expire_time) = if let Ok(expiry) =
                                command_parts[command_parts.len() - 1].parse::<u64>()
                            {
                                (
                                    command_parts[2..command_parts.len() - 1].join(" "),
                                    Some(Instant::now() + Duration::from_secs(expiry)),
                                )
                            } else {
                                (command_parts[2..].join(" "), None)
                            };

                            let mut db_lock = db.write().await;
                            db_lock.insert(key.clone(), (value.clone(), expire_time));

                            // REPLICATION (only if leader and not forwarded)
                            if matches!(config.role, NodeRole::Leader) && !is_forwarded {
                                let peers = config.peers.clone();
                                let req = received.clone();

                                tokio::spawn(async move {
                                    replicate_to_followers(&peers, &req).await;
                                });
                            }

                            format!("OK: {} set to {}\n", key, value)
                        }
                    }
                    "GET" => {
                        if command_parts.len() != 2 {
                            "Usage: GET key\n".to_string()
                        } else {
                            let db_read_lock = db.read().await;
                            match db_read_lock.get(&key) {
                                Some((value, expiry)) => {
                                    if let Some(expire_time) = expiry {
                                        if Instant::now() > *expire_time {
                                            drop(db_read_lock); // Drop read lock before acquiring write lock to avoid deadlock
                                            let mut db_lock = db.write().await;
                                            db_lock.remove(&key);
                                            format!("Error: {} has expired\n", key)
                                        } else {
                                            format!("OK: {} = {}\n", key, value)
                                        }
                                    } else {
                                        format!("OK: {} = {}\n", key, value)
                                    }
                                }
                                None => format!("Error: {} not found\n", key),
                            }
                        }
                    }
                    "DELETE" => {
                        if command_parts.len() != 2 {
                            "Usage: DELETE key\n".to_string()
                        } else {
                            let mut db_lock = db.write().await;
                            if db_lock.remove(&key).is_some() {
                                if matches!(config.role, NodeRole::Leader) && !is_forwarded {
                                    let peers = config.peers.clone();
                                    let req = received.clone();

                                    tokio::spawn(async move {
                                        replicate_to_followers(&peers, &req).await;
                                    });
                                }
                                format!("OK: {} deleted\n", key)
                            } else {
                                format!("Error: {} not found\n", key)
                            }
                        }
                    }
                    "KEYS" => {
                        let db_lock = db.read().await;
                        if db_lock.is_empty() {
                            "No keys found\n".to_string()
                        } else {
                            let keys: Vec<String> = db_lock.keys().cloned().collect();
                            format!("OK: Keys: {}\n", keys.join(", "))
                        }
                    }
                    "EXISTS" => {
                        if command_parts.len() != 2 {
                            "Usage: EXISTS key\n".to_string()
                        } else {
                            let db_lock = db.read().await;
                            if db_lock.contains_key(&key) {
                                format!("OK: {} exists\n", key)
                            } else {
                                format!("OK: {} does not exist\n", key)
                            }
                        }
                    }
                    "TTL" => {
                        if command_parts.len() != 2 {
                            "Usage: TTL key\n".to_string()
                        } else {
                            let db_lock = db.read().await;
                            match db_lock.get(&key) {
                                Some((_, Some(expiry))) => {
                                    let ttl =
                                        expiry.saturating_duration_since(Instant::now()).as_secs();
                                    format!("OK: {} TTL = {} seconds\n", key, ttl)
                                }
                                Some((_, None)) => format!("OK: {} has no expiration\n", key),
                                None => format!("Error: {} not found\n", key),
                            }
                        }
                    }
                    "PERSIST" => {
                        if command_parts.len() != 2 {
                            "Usage: PERSIST key\n".to_string()
                        } else {
                            let mut db_lock = db.write().await;
                            match db_lock.get_mut(&key) {
                                Some((_, expiry)) => {
                                    *expiry = None;

                                    if matches!(config.role, NodeRole::Leader) && !is_forwarded {
                                        let peers = config.peers.clone();
                                        let req = received.clone();

                                        tokio::spawn(async move {
                                            replicate_to_followers(&peers, &req).await;
                                        });
                                    }
                                    format!("OK: {} is now persistent\n", key)
                                }
                                None => format!("Error: {} not found\n", key),
                            }
                        }
                    }
                    _ => "ERR unknown command\n".to_string(),
                };

                let response = format!("[{}] {}", timestamp, response);

                // Echo back
                if let Err(e) = socket.write_all(response.as_bytes()).await {
                    println!("Write error: {}", e);
                    return;
                }
            }
            Err(e) => {
                println!("Error from {}: {}", addr, e);
                return;
            }
        }
    }
}

async fn replicate_to_followers(peers: &[String], request: &str) {
    for peer in peers {
        let req = format!("FORWARD {}\n", request);

        tokio::spawn({
            let peer = peer.clone();
            let req = req.clone();
            let request = request.to_string();

            async move {

                println!("[REPLICATION] Sending '{}' → {}", request, peer);

                let response = forward_with_retry(&peer, &req, 3).await;

                println!("[REPLICATION] Response from {} → {}", peer, response.trim());
            }
        });
    }
}

async fn forward_with_retry(node_addr: &str, request: &str, retries: usize) -> String {
    let mut attempt = 0;

    loop {
        attempt += 1;

        println!(
            "[RETRY] Attempt {} → sending '{}' to {}",
            attempt, request.trim(), node_addr
        );

        let result = forward_request(node_addr, request).await;

        if !result.starts_with("ERR") {
            return result;
        }

        if attempt >= retries {
            println!(
                "[RETRY] Failed after {} attempts → {}",
                retries, node_addr
            );
            return result;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn forward_request(node_addr: &str, request: &str) -> String {
    match timeout(Duration::from_secs(2), TcpStream::connect(node_addr)).await {
        Ok(Ok(mut stream)) => {
            if let Err(_) = stream.write_all(request.as_bytes()).await {
                return "ERR forwarding failed\n".to_string();
            }

            let mut buffer = [0; 1024];
            match stream.read(&mut buffer).await {
                Ok(n) => String::from_utf8_lossy(&buffer[..n]).to_string(),
                Err(_) => "ERR read failed\n".to_string(),
            }
        }
        Ok(Err(e)) => format!("ERR connection timed out: {}\n", e),
        Err(_) => "ERR could not connect\n".to_string(),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        // eprintln!("Usage: {} <config_file>", args[0]);
        eprintln!("Usage: cargo run <port> <leader|follower>");
        return;
    }

    let port = &args[1];
    let addr = format!("127.0.0.1:{}", port);

    let role = match args[2].as_str() {
        "leader" => NodeRole::Leader,
        "follower" => NodeRole::Follower,
        _ => {
            eprintln!("Usage: cargo run <port> <leader|follower>");
            return;
        }
    };

    // hardcoded peers (for now)
    let peers = vec![
        "127.0.0.1:8080".to_string(),
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ]
    .into_iter()
    .filter(|p| p != &addr)
    .collect::<Vec<_>>();

    // hardcode leader (for now)
    let leader_addr = "127.0.0.1:8080".to_string();

    let config = Arc::new(NodeConfig {
        addr,
        peers,
        role,
        leader_addr,
    });

    let listener = TcpListener::bind(&config.addr).await.unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());

    let db = Arc::new(RwLock::new(
        HashMap::<String, (String, Option<Instant>)>::new(),
    ));

    let (tx, mut rx) = mpsc::channel::<String>(100);

    let connection_count = Arc::new(AtomicUsize::new(0));

    // Background logger task
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            println!("[LOG]: {}", msg);
        }
    });

    let db_cleanup = db.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut db_lock = db_cleanup.write().await;
            let now = Instant::now();

            db_lock.retain(|key, (_, expiry)| {
                if let Some(expire_time) = expiry {
                    if now > *expire_time {
                        println!("Key '{}' has expired and was removed", key);
                        return false;
                    }
                }
                true
            });
        }
    });

    loop {
        let (socket, addr) = listener.accept().await.unwrap();

        let connection_count = connection_count.clone();
        let tx = tx.clone();
        let db = db.clone();
        let config = config.clone();

        tokio::spawn(async move {
            let log_msg = format!("Client connected: {}", addr);
            if let Err(e) = tx.send(log_msg).await {
                println!("Failed to send log: {}", e);
            };

            handle_client(socket, connection_count, db, config).await;
        });
    }
}

// Day 4 - Distributed Key-Value Store with Consistent Hashing

// use tokio::{
//     io::{AsyncReadExt, AsyncWriteExt},
//     net::{TcpListener, TcpStream},
//     sync::{RwLock, mpsc},
//     time::timeout,
// };

// use std::{
//     collections::{HashMap, hash_map::DefaultHasher},
//     hash::{Hash, Hasher},
//     sync::{
//         Arc,
//         atomic::{AtomicUsize, Ordering},
//     },
//     time::{Duration, Instant},
// };

// use chrono::Local;

// const MAX_MSG_SIZE: usize = 512;

// #[derive(Debug, Clone)]
// struct NodeConfig {
//     addr: String,
//     peers: Vec<String>,
// }

// async fn handle_client(
//     mut socket: TcpStream,
//     connection_count: Arc<AtomicUsize>,
//     db: Arc<RwLock<HashMap<String, (String, Option<Instant>)>>>,
//     config: Arc<NodeConfig>,
// ) {
//     let addr = socket.peer_addr().unwrap();

//     let current = connection_count.fetch_add(1, Ordering::SeqCst) + 1;
//     println!("Connected: {} | Active: {}", addr, current);

//     let mut buffer = [0; 1024];

//     loop {
//         match socket.read(&mut buffer).await {
//             Ok(0) => {
//                 let current = connection_count.fetch_sub(1, Ordering::SeqCst) - 1;
//                 println!("Disconnected: {} | Active: {}", addr, current);
//                 return;
//             }
//             Ok(n) => {
//                 if n > MAX_MSG_SIZE {
//                     let err = "Message too large\n";
//                     if let Err(e) = socket.write_all(err.as_bytes()).await {
//                         println!("Write error: {}", e);
//                         return;
//                     }
//                     println!("Rejected large message from {}", addr);
//                     continue;
//                 }

//                 let received = String::from_utf8_lossy(&buffer[..n]).trim().to_string();
//                 let now = Local::now();
//                 let timestamp = now.format("%Y-%m-%d %H:%M:%S");

//                 println!("[{}][{}] Received: {}", timestamp, addr, received);

//                 let parts: Vec<&str> = received.split_whitespace().collect();

//                 if parts.is_empty() {
//                     let err = "Empty command\n";
//                     if let Err(e) = socket.write_all(err.as_bytes()).await {
//                         println!("Write error: {}", e);
//                         return;
//                     }
//                     println!("Received empty command from {}", addr);
//                     continue;
//                 }

//                 // let is_forwarded = parts[0] == "FORWARD";

//                 // // let command = parts[0].to_uppercase();
//                 // let command = if is_forwarded {
//                 //     parts[1].to_uppercase()
//                 // } else {
//                 //     parts[0].to_uppercase()
//                 // };

//                 // // let key = parts.get(1).unwrap_or(&"");
//                 // let key = if is_forwarded {
//                 //     parts.get(2).unwrap_or(&"")
//                 // } else {
//                 //     parts.get(1).unwrap_or(&"")
//                 // }.to_string();

//                 let (command_parts, is_forwarded) = if parts[0] == "FORWARD" {
//                     (&parts[1..], true)
//                 } else {
//                     (&parts[..], false)
//                 };

//                 let command = command_parts[0].to_uppercase();
//                 let key = command_parts.get(1).unwrap_or(&"").to_string();

//                 let mut all_nodes = config.peers.clone();
//                 all_nodes.push(config.addr.clone());

//                 all_nodes.sort(); // without sorting, the same key might be assigned to different nodes on different servers, causing inconsistency. Sorting ensures that all nodes will have the same order of peers and thus the same key distribution.

//                 let target_node = get_node_for_key(&key, &all_nodes);

//                 if !is_forwarded && target_node != config.addr {
//                     let forward_req = format!("FORWARD {}\n", received);
//                     let response = forward_request(&target_node, &forward_req).await;
//                     socket.write_all(response.as_bytes()).await.unwrap();
//                     continue;
//                 }

//                 let response = match command.as_str() {
//                     "SET" => {
//                         if command_parts.len() < 3 {
//                             "Usage: SET key value\n".to_string()
//                         } else {
//                             let (value, expire_time) =
//                                 if let Ok(expiry) = command_parts[command_parts.len() - 1].parse::<u64>() {
//                                     (
//                                         command_parts[2..command_parts.len() - 1].join(" "),
//                                         Some(Instant::now() + Duration::from_secs(expiry)),
//                                     )
//                                 } else {
//                                     (command_parts[2..].join(" "), None)
//                                 };

//                             let mut db_lock = db.write().await;
//                             db_lock.insert(key.clone(), (value.clone(), expire_time));
//                             format!("OK: {} set to {}\n", key, value)
//                         }
//                     }
//                     "GET" => {
//                         if command_parts.len() != 2 {
//                             "Usage: GET key\n".to_string()
//                         } else {
//                             let db_read_lock = db.read().await;
//                             match db_read_lock.get(&key) {
//                                 Some((value, expiry)) => {
//                                     if let Some(expire_time) = expiry {
//                                         if Instant::now() > *expire_time {
//                                             drop(db_read_lock); // Drop read lock before acquiring write lock to avoid deadlock
//                                             let mut db_lock = db.write().await;
//                                             db_lock.remove(&key);
//                                             format!("Error: {} has expired\n", key)
//                                         } else {
//                                             format!("OK: {} = {}\n", key, value)
//                                         }
//                                     } else {
//                                         format!("OK: {} = {}\n", key, value)
//                                     }
//                                 }
//                                 None => format!("Error: {} not found\n", key),
//                             }
//                         }
//                     }
//                     "DELETE" => {
//                         if command_parts.len() != 2 {
//                             "Usage: DELETE key\n".to_string()
//                         } else {
//                             let mut db_lock = db.write().await;
//                             if db_lock.remove(&key).is_some() {
//                                 format!("OK: {} deleted\n", key)
//                             } else {
//                                 format!("Error: {} not found\n", key)
//                             }
//                         }
//                     }
//                     "KEYS" => {
//                         let db_lock = db.read().await;
//                         if db_lock.is_empty() {
//                             "No keys found\n".to_string()
//                         } else {
//                             let keys: Vec<String> = db_lock.keys().cloned().collect();
//                             format!("OK: Keys: {}\n", keys.join(", "))
//                         }
//                     }
//                     "EXISTS" => {
//                         if command_parts.len() != 2 {
//                             "Usage: EXISTS key\n".to_string()
//                         } else {
//                             let db_lock = db.read().await;
//                             if db_lock.contains_key(&key) {
//                                 format!("OK: {} exists\n", key)
//                             } else {
//                                 format!("OK: {} does not exist\n", key)
//                             }
//                         }
//                     }
//                     "TTL" => {
//                         if command_parts.len() != 2 {
//                             "Usage: TTL key\n".to_string()
//                         } else {
//                             let db_lock = db.read().await;
//                             match db_lock.get(&key) {
//                                 Some((_, Some(expiry))) => {
//                                     let ttl =
//                                         expiry.saturating_duration_since(Instant::now()).as_secs();
//                                     format!("OK: {} TTL = {} seconds\n", key, ttl)
//                                 }
//                                 Some((_, None)) => format!("OK: {} has no expiration\n", key),
//                                 None => format!("Error: {} not found\n", key),
//                             }
//                         }
//                     }
//                     "PERSIST" => {
//                         if command_parts.len() != 2 {
//                             "Usage: PERSIST key\n".to_string()
//                         } else {
//                             let mut db_lock = db.write().await;
//                             match db_lock.get_mut(&key) {
//                                 Some((_, expiry)) => {
//                                     *expiry = None;
//                                     format!("OK: {} is now persistent\n", key)
//                                 }
//                                 None => format!("Error: {} not found\n", key),
//                             }
//                         }
//                     }
//                     _ => "ERR unknown command\n".to_string(),
//                 };

//                 let response = format!("[{}] {}", timestamp, response);

//                 // Echo back
//                 if let Err(e) = socket.write_all(response.as_bytes()).await {
//                     println!("Write error: {}", e);
//                     return;
//                 }
//             }
//             Err(e) => {
//                 println!("Error from {}: {}", addr, e);
//                 return;
//             }
//         }
//     }
// }

// fn get_node_for_key(key: &str, nodes: &[String]) -> String {
//     let mut hasher = DefaultHasher::new();
//     key.hash(&mut hasher);

//     let hash = hasher.finish();
//     let idx = (hash as usize) % nodes.len();

//     nodes[idx].clone()
// }

// async fn forward_request(node_addr: &str, request: &str) -> String {
//     match timeout(Duration::from_secs(2), TcpStream::connect(node_addr)).await {
//         Ok(Ok(mut stream)) => {
//             if let Err(_) = stream.write_all(request.as_bytes()).await {
//                 return "ERR forwarding failed\n".to_string();
//             }

//             let mut buffer = [0; 1024];
//             match stream.read(&mut buffer).await {
//                 Ok(n) => String::from_utf8_lossy(&buffer[..n]).to_string(),
//                 Err(_) => "ERR read failed\n".to_string(),
//             }
//         }
//         Ok(Err(e)) => format!("ERR connection timed out: {}\n", e),
//         Err(_) => "ERR could not connect\n".to_string(),
//     }
// }

// #[tokio::main]
// async fn main() {
//     let args: Vec<String> = std::env::args().collect();

//     if args.len() < 2 {
//         // eprintln!("Usage: {} <config_file>", args[0]);
//         eprintln!("Usage: cargo run <port>");
//         return;
//     }

//     let port = &args[1];
//     let addr = format!("127.0.0.1:{}", port);

//     // hardcoded peers (for now)
//     let peers = vec![
//         "127.0.0.1:8080".to_string(),
//         "127.0.0.1:8081".to_string(),
//         "127.0.0.1:8082".to_string(),
//     ]
//     .into_iter()
//     .filter(|p| p != &addr)
//     .collect::<Vec<_>>();

//     let config = Arc::new(NodeConfig { addr, peers });

//     let listener = TcpListener::bind(&config.addr).await.unwrap();
//     println!("Listening on {}", listener.local_addr().unwrap());

//     let db = Arc::new(RwLock::new(
//         HashMap::<String, (String, Option<Instant>)>::new(),
//     ));

//     let (tx, mut rx) = mpsc::channel::<String>(100);

//     let connection_count = Arc::new(AtomicUsize::new(0));

//     // Background logger task
//     tokio::spawn(async move {
//         while let Some(msg) = rx.recv().await {
//             println!("[LOG]: {}", msg);
//         }
//     });

//     let db_cleanup = db.clone();

//     tokio::spawn(async move {
//         loop {
//             tokio::time::sleep(Duration::from_secs(5)).await;
//             let mut db_lock = db_cleanup.write().await;
//             let now = Instant::now();

//             db_lock.retain(|key, (_, expiry)| {
//                 if let Some(expire_time) = expiry {
//                     if now > *expire_time {
//                         println!("Key '{}' has expired and was removed", key);
//                         return false;
//                     }
//                 }
//                 true
//             });
//         }
//     });

//     loop {
//         let (socket, addr) = listener.accept().await.unwrap();

//         let connection_count = connection_count.clone();
//         let tx = tx.clone();
//         let db = db.clone();
//         let config = config.clone();

//         tokio::spawn(async move {
//             let log_msg = format!("Client connected: {}", addr);
//             if let Err(e) = tx.send(log_msg).await {
//                 println!("Failed to send log: {}", e);
//             };

//             handle_client(socket, connection_count, db, config).await;
//         });
//     }
// }

// Day 3 - In-Memory Key-Value Store with Expiration

// use tokio::{
//     io::{AsyncReadExt, AsyncWriteExt},
//     net::{TcpListener, TcpStream},
//     sync::{Mutex, RwLock, mpsc},
// };

// use std::{
//     collections::HashMap,
//     sync::{
//         Arc,
//         atomic::{AtomicUsize, Ordering},
//     },
//     time::{Duration, Instant},
// };

// use chrono::Local;

// const MAX_MSG_SIZE: usize = 512;

// async fn handle_client(
//     mut socket: TcpStream,
//     connection_count: Arc<AtomicUsize>,
//     db: Arc<RwLock<HashMap<String, (String, Option<Instant>)>>>,
// ) {
//     let addr = socket.peer_addr().unwrap();

//     let current = connection_count.fetch_add(1, Ordering::SeqCst) + 1;
//     println!("Connected: {} | Active: {}", addr, current);

//     let mut buffer = [0; 1024];

//     loop {
//         match socket.read(&mut buffer).await {
//             Ok(0) => {
//                 let current = connection_count.fetch_sub(1, Ordering::SeqCst) - 1;
//                 println!("Disconnected: {} | Active: {}", addr, current);
//                 return;
//             }
//             Ok(n) => {
//                 if n > MAX_MSG_SIZE {
//                     let err = "Message too large\n";
//                     if let Err(e) = socket.write_all(err.as_bytes()).await {
//                         println!("Write error: {}", e);
//                         return;
//                     }
//                     println!("Rejected large message from {}", addr);
//                     continue;
//                 }

//                 let received = String::from_utf8_lossy(&buffer[..n]).trim().to_string();
//                 let now = Local::now();
//                 let timestamp = now.format("%Y-%m-%d %H:%M:%S");

//                 println!("[{}][{}] Received: {}", timestamp, addr, received);

//                 let parts: Vec<&str> = received.split_whitespace().collect();

//                 if parts.is_empty() {
//                     let err = "Empty command\n";
//                     if let Err(e) = socket.write_all(err.as_bytes()).await {
//                         println!("Write error: {}", e);
//                         return;
//                     }
//                     println!("Received empty command from {}", addr);
//                     continue;
//                 }

//                 let command = parts[0].to_uppercase();

//                 let response = match command.as_str() {
//                     "SET" => {
//                         if parts.len() < 3 {
//                             "Usage: SET key value\n".to_string()
//                         } else {
//                             let key = parts[1].to_string();

//                             let (value, expire_time) =
//                                 if parts[parts.len() - 1].parse::<u64>().is_ok() {
//                                     (
//                                         parts[2..parts.len() - 1].join(" "),
//                                         Some(
//                                             Instant::now()
//                                                 + Duration::from_secs(
//                                                     parts[parts.len() - 1].parse::<u64>().unwrap(),
//                                                 ),
//                                         ),
//                                     )
//                                 } else {
//                                     (parts[2..].join(" "), None)
//                                 };

//                             let mut db_lock = db.write().await;
//                             db_lock.insert(key.clone(), (value.clone(), expire_time));
//                             format!("OK: {} set to {}\n", key, value)
//                         }
//                     }

//                     // DeadLock example - if we try to acquire write lock while holding read lock on the same db, it will cause a deadlock.
//                     // Better to drop the read lock before acquiring the write lock. Below is an example of how not to do it:

//                     "GET" => {
//                         if parts.len() != 2 {
//                             "Usage: GET key\n".to_string()
//                         } else {
//                             let key = parts[1];
//                             let db_lock = db.read().await;
//                             match db_lock.get(key) {
//                                 Some((value, expiry)) => {
//                                     if let Some(expire_time) = expiry {
//                                         if Instant::now() > *expire_time {
//                                             let mut db_lock = db.write().await;
//                                             db_lock.remove(key);
//                                             format!("Error: {} has expired\n", key)
//                                         } else {
//                                             format!("OK: {} = {}\n", key, value)
//                                         }
//                                     } else {
//                                         format!("OK: {} = {}\n", key, value)
//                                     }
//                                 }
//                                 None => format!("Error: {} not found\n", key),
//                             }
//                         }
//                     }
//                     "DELETE" => {
//                         if parts.len() != 2 {
//                             "Usage: DELETE key\n".to_string()
//                         } else {
//                             let key = parts[1].to_string();
//                             let mut db_lock = db.write().await;
//                             if db_lock.remove(&key).is_some() {
//                                 format!("OK: {} deleted\n", key)
//                             } else {
//                                 format!("Error: {} not found\n", key)
//                             }
//                         }
//                     }
//                     "KEYS" => {
//                         let db_lock = db.read().await;
//                         if db_lock.is_empty() {
//                             "No keys found\n".to_string()
//                         } else {
//                             let keys: Vec<String> = db_lock.keys().cloned().collect();
//                             format!("OK: Keys: {}\n", keys.join(", "))
//                         }
//                     }
//                     "EXISTS" => {
//                         if parts.len() != 2 {
//                             "Usage: EXISTS key\n".to_string()
//                         } else {
//                             let key = parts[1];
//                             let db_lock = db.read().await;
//                             if db_lock.contains_key(key) {
//                                 format!("OK: {} exists\n", key)
//                             } else {
//                                 format!("OK: {} does not exist\n", key)
//                             }
//                         }
//                     }
//                     "TTL" => {
//                         if parts.len() != 2 {
//                             "Usage: TTL key\n".to_string()
//                         } else {
//                             let key = parts[1];
//                             let db_lock = db.read().await;
//                             match db_lock.get(key) {
//                                 Some((_, Some(expiry))) => {
//                                     let ttl =
//                                         expiry.saturating_duration_since(Instant::now()).as_secs();
//                                     format!("OK: {} TTL = {} seconds\n", key, ttl)
//                                 }
//                                 Some((_, None)) => format!("OK: {} has no expiration\n", key),
//                                 None => format!("Error: {} not found\n", key),
//                             }
//                         }
//                     }
//                     "PERSIST" => {
//                         if parts.len() != 2 {
//                             "Usage: PERSIST key\n".to_string()
//                         } else {
//                             let key = parts[1].to_string();
//                             let mut db_lock = db.write().await;
//                             match db_lock.get_mut(&key) {
//                                 Some((_, expiry)) => {
//                                     *expiry = None;
//                                     format!("OK: {} is now persistent\n", key)
//                                 }
//                                 None => format!("Error: {} not found\n", key),
//                             }
//                         }
//                     }
//                     _ => "ERR unknown command\n".to_string(),
//                 };

//                 let response = format!("[{}] {}", timestamp, response);

//                 // Echo back
//                 if let Err(e) = socket.write_all(response.as_bytes()).await {
//                     println!("Write error: {}", e);
//                     return;
//                 }
//             }
//             Err(e) => {
//                 println!("Error from {}: {}", addr, e);
//                 return;
//             }
//         }
//     }
// }

// #[tokio::main]
// async fn main() {
//     let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
//     println!("Listening on {}", listener.local_addr().unwrap());

//     let db = Arc::new(RwLock::new(
//         HashMap::<String, (String, Option<Instant>)>::new(),
//     ));

//     let (tx, mut rx) = mpsc::channel::<String>(100);

//     let connection_count = Arc::new(AtomicUsize::new(0));

//     // Background logger task
//     tokio::spawn(async move {
//         while let Some(msg) = rx.recv().await {
//             println!("[LOG]: {}", msg);
//         }
//     });

//     let db_cleanup = db.clone();

//     tokio::spawn(async move {
//         loop {
//             tokio::time::sleep(Duration::from_secs(5)).await;
//             let mut db_lock = db_cleanup.write().await;
//             let now = Instant::now();

//             db_lock.retain(|key, (_, expiry)| {
//                 if let Some(expire_time) = expiry {
//                     if now > *expire_time {
//                         println!("Key '{}' has expired and was removed", key);
//                         return false;
//                     }
//                 }
//                 true
//             });
//         }
//     });

//     loop {
//         let (socket, addr) = listener.accept().await.unwrap();
//         let connection_count = connection_count.clone();

//         let tx = tx.clone();
//         let db = db.clone();

//         tokio::spawn(async move {
//             let log_msg = format!("Client connected: {}", addr);
//             if let Err(e) = tx.send(log_msg).await {
//                 println!("Failed to send log: {}", e);
//             };

//             handle_client(socket, connection_count, db).await;
//         });
//     }
// }

// Day 2 - Async TCP Echo Server with Logging

// use tokio::{
//     io::{AsyncReadExt, AsyncWriteExt},
//     net::{TcpListener, TcpStream},
//     sync::{mpsc, Mutex},
// };

// use std::{
//     collections::HashMap,
//     sync::{
//     Arc,
//     atomic::{AtomicUsize, Ordering}},
// };

// use chrono::Local;

// const MAX_MSG_SIZE: usize = 512;

// async fn handle_client(mut socket: TcpStream, connection_count: Arc<AtomicUsize>) {
//     let addr = socket.peer_addr().unwrap();

//     let current = connection_count.fetch_add(1, Ordering::SeqCst) + 1;
//     println!("Connected: {} | Active: {}", addr, current);

//     let mut buffer = [0; 1024];

//     loop {
//         match socket.read(&mut buffer).await {
//             Ok(0) => {
//                 let current = connection_count.fetch_sub(1, Ordering::SeqCst) - 1;
//                 println!("Disconnected: {} | Active: {}", addr, current);
//                 return;
//             }
//             Ok(n) => {
//                 if n > MAX_MSG_SIZE {
//                     let err = "Message too large\n";
//                     socket.write_all(err.as_bytes()).await.unwrap();
//                     println!("Rejected large message from {}", addr);
//                     continue;
//                 }

//                 // let received = String::from_utf8_lossy(&buffer[..n]);
//                 let received = String::from_utf8_lossy(&buffer[..n]).trim().to_string();
//                 let now = Local::now();
//                 let timestamp = now.format("%Y-%m-%d %H:%M:%S");

//                 println!("[{}][{}] Received: {}", timestamp, addr, received);

//                 let response = format!("[{}] {}", timestamp, received);

//                 // Echo back
//                 if let Err(e) = socket.write_all(response.as_bytes()).await {
//                     println!("Write error: {}", e);
//                     return;
//                 }
//             }
//             Err(e) => {
//                 println!("Error from {}: {}", addr, e);
//                 return;
//             }
//         }
//     }
// }

// #[tokio::main]
// async fn main() {
//     let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
//     println!("Listening on {}", listener.local_addr().unwrap());

//     let (tx, mut rx) = mpsc::channel::<String>(100);

//     let connection_count = Arc::new(AtomicUsize::new(0));

//     // Background logger task
//     tokio::spawn(async move {
//         while let Some(msg) = rx.recv().await {
//             println!("[LOG]: {}", msg);
//         }
//     });

//     loop {
//         let (socket, addr) = listener.accept().await.unwrap();
//         let connection_count = connection_count.clone();

//         let tx = tx.clone();

//         tokio::spawn(async move {
//             let log_msg = format!("Client connected: {}", addr);
//             if let Err(e) = tx.send(log_msg).await {
//                 println!("Failed to send log: {}", e);
//             };

//             handle_client(socket, connection_count).await;
//         });
//     }
// }

//// Day 1 - TCP Echo Server and Client

// use std::io::{Read, Write};
// use std::net::{TcpListener, TcpStream};
// use std::thread;

// fn handle_client(mut stream: TcpStream) {
//     let mut buffer = [0; 1024];

//     loop {
//         match stream.read(&mut buffer) {
//             Ok(0) => {
//                 println!("Client disconnected");
//                 break;
//             }
//             Ok(n) => {
//                 let received = String::from_utf8_lossy(&buffer[..n]);
//                 println!("Received: {}", received);

//                 // Echo back
//                 stream.write_all(received.to_uppercase().as_bytes()).unwrap();
//             }
//             Err(e) => {
//                 println!("Error: {}", e);
//                 break;
//             }
//         }
//     }
// }

// fn main() {
//     let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
//     // println!("Server running on 127.0.0.1:8080");
//     println!("Listening on {}", listener.local_addr().unwrap());

//     for stream in listener.incoming() {
//         match stream {
//             Ok(stream) => {
//                 println!("New connection: {}", stream.peer_addr().unwrap());
//                 // handle_client(stream); -- single threaded

//                 thread::spawn(|| { // -- concurrency (basic threads)
//                     handle_client(stream);
//                 });
//             }
//             Err(e) => {
//                 println!("Connection failed: {}", e);
//             }
//         }
//     }
// }
