use std::io::{Read, Write};
use std::net::TcpStream;

fn main() {
    let mut leader = TcpStream::connect("127.0.0.1:8080").unwrap();

    let commands = vec![
        "SET user1 Alice 10\n",
        "SET user2 Bob 10\n",
        "SET user3 Charlie 10\n",
        "SET name Jayyy\n",
        "GET user1\n",
        "GET user2\n",
        "GET user3\n",
        "GET name\n",
        "DELETE name\n",
        "GET name\n",
        "SET name Jayyy 10\n",
        "GET name\n",
        "TTL name\n",
        "EXISTS name\n",
        "PERSIST name\n",
        "TTL name\n",
        "SET company Jayyy & Co 7\n",
    ];

    for cmd in commands {
        std::thread::sleep(std::time::Duration::from_secs(1));
        leader.write_all(cmd.as_bytes()).unwrap();

        let mut buffer = [0; 1024];
        let n = leader.read(&mut buffer).unwrap();

        println!("Leader Response: {}", String::from_utf8_lossy(&buffer[..n]));
    }

    let mut follower = TcpStream::connect("127.0.0.1:8081").unwrap();

    follower.write_all(b"GET name\n").unwrap();
    let mut buffer = [0; 1024];
    let n = follower.read(&mut buffer).unwrap();
    println!("Follower Response: {}", String::from_utf8_lossy(&buffer[..n]));
}

// Day 1&2 - TCP Echo Client

// use std::io::{Read, Write};
// use std::net::TcpStream;

// fn main() {
//     for i in 1..=10 {
//         let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();

//         // let message = "hello from client {}".replace("{}", &i.to_string());
//         let message = format!("hello from client {}\n", i);
//         stream.write_all(message.as_bytes()).unwrap();

//         let mut buffer = [0; 1024];
//         let n = stream.read(&mut buffer).unwrap();

//         println!(
//             "Received from server: {}",
//             String::from_utf8_lossy(&buffer[..n])
//         );
//     }
// }
