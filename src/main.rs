#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut msg_size = [0; 4];
                stream.read(&mut msg_size).unwrap();
                let mut header = [0; 8];
                stream.read(&mut header).unwrap();
                println!("received header: {:?}", header);
                println!("received message of size: {:?}", msg_size);

                let corr_id = &header[4..8];
                println!("correlation id: {:?}", i32::from_be_bytes(corr_id.try_into().unwrap()));

                let res_buff = [
                    &[0, 0, 0, 0],
                    corr_id
                ].concat();
                stream.write(&res_buff).unwrap();
                stream.flush().unwrap();
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
