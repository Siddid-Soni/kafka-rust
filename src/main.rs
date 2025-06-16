#![allow(unused_imports)]
use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use anyhow::Result;
use bytes::buf;

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
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(anyhow::anyhow!("RequestV2: Not enough bytes to parse"));
        }
        let message_size = u32::from_be_bytes(bytes[0..4].try_into()?);
        let request_api_key = u16::from_be_bytes(bytes[4..6].try_into()?);
        let request_api_version = u16::from_be_bytes(bytes[6..8].try_into()?);
        let correlation_id = i32::from_be_bytes(bytes[8..12].try_into()?);

        Ok(RequestV2 {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
        })
    }
}


#[derive(Debug)]
struct ResponseV0 {
    message_size: u32,
    correlation_id: i32,
    error_code: i16, // Assuming error_code is part of the response
    // tag_buffer: Vec<u8>
    api_versions: Vec<ApiVersion>, // Placeholder for version array
}

impl ResponseV0 {
    fn to_bytes(&mut self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        // Reserve 4 bytes for message_size (will be filled later)
        buffer.extend_from_slice(&[0u8; 4]);
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&(self.api_versions.len() as u8 + 1).to_be_bytes());
        for version in &self.api_versions {
            buffer.extend_from_slice(&version.api_key.to_be_bytes());
            buffer.extend_from_slice(&version.min_version.to_be_bytes());
            buffer.extend_from_slice(&version.max_version.to_be_bytes());
            buffer.push(0); // Placeholder for tag buffer
        }
        // Add a final 0 byte to indicate the end of the version array
        buffer.extend_from_slice(&0u32.to_be_bytes());
        buffer.push(0);
        // Calculate message size (total buffer length - 4 bytes for message_size field)
        let message_size = (buffer.len() - 4) as u32;
        buffer[0..4].copy_from_slice(&message_size.to_be_bytes());
        
        Ok(buffer)
    }

    fn from_request(request: &RequestV2) -> Self {
        ResponseV0 {
            message_size: 0, // This will be set later
            correlation_id: request.correlation_id,
            error_code: 0, // Default to no error
            api_versions: vec![ApiVersion { api_key: 18, min_version: 0, max_version: 4 }], // Default to empty version array
        }
    }

    fn set_size(&mut self) {
        self.message_size = self.to_bytes().unwrap().len() as u32 - 4; // 4 bytes for message_size
    }
    
}

#[derive(Debug)]
struct ApiVersion {
    api_key: u16,
    min_version: u16,
    max_version: u16,
}

async fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                println!("Client disconnected");
                break;
            }
            Ok(size) => {
                let request = RequestV2::from_bytes(&buffer[..size]);
                println!("Received request: {:?}", request);
                let request = match request {
                    Ok(req) => req,
                    Err(e) => {
                        println!("Failed to parse request: {}", e);
                        continue;
                    }
                };
                let mut response = ResponseV0::from_request(&request);

                if ![0,1,2,3,4].contains(&request.request_api_version) {
                    response.error_code = 35;
                }
                response.set_size();
                // Send the response back
                println!("Sending response: {:?}", response);
                let response_bytes = response.to_bytes();
                let response_bytes = match response_bytes {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        println!("Failed to convert response to bytes: {}", e);
                        continue;
                    }
                };

                if let Err(e) = stream.write_all(&response_bytes).await {
                    println!("Failed to write response: {}", e);
                    break;
                }
            }
            Err(e) => {
                println!("Failed to read from stream: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(stream).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
