#![allow(unused_imports)]

mod request;
mod response;

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use anyhow::Result;
use bytes::{buf, BytesMut};


use request::{ReqApiVersionsV4, RequestHeaderV2, ReqDescTopicPartitionV0};
use response::{ResApiVersionsV4, ResDescTopicPartitionV0};

#[derive(Debug)]
enum Response {
    ApiVersions(ResApiVersionsV4),
    DescTopicPartition(ResDescTopicPartitionV0),
}

impl Response {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Response::ApiVersions(res) => res.to_bytes(),
            Response::DescTopicPartition(res) => res.to_bytes(),
        }
    }
}

enum Request {
    ApiVersions(ReqApiVersionsV4),
    DescTopicPartition(ReqDescTopicPartitionV0),
}

impl Request {
    fn from_bytes(header: RequestHeaderV2, bytes: &mut BytesMut) -> Result<Self> {
        match header.request_api_key {
            18 => Ok(Request::ApiVersions(ReqApiVersionsV4::from_bytes(header, bytes)?)),
            0 => Ok(Request::DescTopicPartition(ReqDescTopicPartitionV0::from_bytes(header, bytes)?)),
            _ => Err(anyhow::anyhow!("Unsupported API key: {}", header.request_api_key)),
        }
    }
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
                let mut bytes_mut = BytesMut::from(&buffer[..size]);
                let header = RequestHeaderV2::from_bytes(&mut bytes_mut).unwrap();
                let res = match header.request_api_key {
                    18 => {
                        let req = ReqApiVersionsV4::from_bytes(header, &mut bytes_mut).unwrap();
                        let res = ResApiVersionsV4::from_request(&req);
                        Response::ApiVersions(res)
                    }
                    0 => {
                        let req = ReqDescTopicPartitionV0::from_bytes(header, &mut bytes_mut).unwrap();
                        let res = ResDescTopicPartitionV0::from_request(&req);
                        Response::DescTopicPartition(res)
                    }
                    _ => {
                        println!("Unsupported API key: {}", header.request_api_key);
                        continue;
                    }
                };
                
                
                // Send the response back
                println!("Sending response: {:?}", res);
                let response_bytes = res.to_bytes();
                let response_bytes = match response_bytes {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        println!("Failed to convert response to bytes: {}", e);
                        continue;
                    }
                };

                stream.write_u32(response_bytes.len() as u32).await.unwrap();
                stream.write_all(&response_bytes).await.unwrap();
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
