#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

#[derive(Debug)]
struct RequestV2 {
    message_size: u32,
    request_api_key: u16,
    request_api_version: u16,
    correlation_id: i32,
    // client_id: Option<String>,
    // tag_buffer: Vec<u8>
}

impl RequestV2 {
    fn from_bytes(bytes: &[u8]) -> Self {
        let message_size = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let request_api_key = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
        let request_api_version = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
        
        RequestV2 {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
        }
    }
}

struct ResponseV0 {
    message_size: u32,
    correlation_id: i32,
    // tag_buffer: Vec<u8>
}

impl ResponseV0 {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.message_size.to_be_bytes());
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer
    }
    
}

fn handle_connection(stream: &mut std::net::TcpStream) {
    let mut buffer = [0; 1024];
    match stream.read(&mut buffer) {
        Ok(size) => {
            if size > 0 {
                let request = RequestV2::from_bytes(&buffer[..size]);
                println!("Received request: {:?}", request);
                
                // Create a response
                let response = ResponseV0 {
                    message_size: size_of::<RequestV2>() as u32,
                    correlation_id: request.correlation_id,
                };
                
                // Send the response back
                let response_bytes = response.to_bytes();
                stream.write_all(&response_bytes).unwrap();
            }
        }
        Err(e) => {
            println!("Failed to read from stream: {}", e);
        }
    }

}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                handle_connection(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
