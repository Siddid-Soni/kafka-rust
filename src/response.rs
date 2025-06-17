use crate::request::{ReqApiVersionsV4, ReqDescTopicPartitionV0};
use anyhow::Result;
use bytes::{buf, BytesMut, Buf};

#[derive(Debug)]
pub struct ApiVersion {
    api_key: u16,
    min_version: u16,
    max_version: u16,
}

#[derive(Debug)]
pub struct ResApiVersionsV4 {
    correlation_id: i32,
    error_code: i16, // Assuming error_code is part of the response
    api_versions: Vec<ApiVersion>, // Placeholder for version array
    throttle_time_ms: u32, // Placeholder for throttle time
}

impl ResApiVersionsV4 {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        // Reserve 4 bytes for message_size (will be filled later)
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
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.push(0);
        // Calculate message size (total buffer length - 4 bytes for message_size field)
        
        Ok(buffer)
    }

    pub fn from_request(request: &ReqApiVersionsV4) -> Self {
        ResApiVersionsV4 {
            correlation_id: request.header.correlation_id,
            error_code: 0, // Default to no error
            api_versions: vec![ApiVersion { api_key: 18, min_version: 0, max_version: 4 },
                            ApiVersion { api_key: 75, min_version: 0, max_version: 0 }],
            throttle_time_ms: 0, // Default to no throttle
        }
    }    
}

#[derive(Debug, Default)]
pub struct Partition {
    pub partition_id: i32,
    pub leader_id: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
    pub partition_error_code: i16,
}

#[derive(Debug, Default)]
pub struct Topic {
    pub error_code: i16,
    pub topic_name: String,
    pub topic_id: u128,
    pub is_internal: bool,
    pub partitions: Option<Vec<Partition>>,
    pub topic_operations: u32,
}

impl Topic {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        // error_code
        buffer.extend_from_slice(&self.error_code.to_be_bytes());

        let topic_name_bytes = self.topic_name.as_bytes();
        // topic_name_len
        buffer.push(topic_name_bytes.len() as u8);
        // topic_name
        buffer.extend_from_slice(topic_name_bytes);
        // topic_id
        buffer.extend_from_slice(&self.topic_id.to_be_bytes());
        // topic_is_internal
        buffer.push(self.is_internal as u8);
        
        if let Some(partitions) = &self.partitions {
            // topic_partition_count
            buffer.push(partitions.len() as u8 + 1);

            for partition in partitions {
                buffer.extend_from_slice(&partition.partition_id.to_be_bytes());
                buffer.extend_from_slice(&partition.leader_id.to_be_bytes());
                buffer.push(partition.replica_nodes.len() as u8);
                for node in &partition.replica_nodes {
                    buffer.extend_from_slice(&node.to_be_bytes());
                }
                buffer.push(partition.isr_nodes.len() as u8);
                for node in &partition.isr_nodes {
                    buffer.extend_from_slice(&node.to_be_bytes());
                }
                buffer.push(partition.offline_replicas.len() as u8);
                for node in &partition.offline_replicas {
                    buffer.extend_from_slice(&node.to_be_bytes());
                }
                buffer.extend_from_slice(&partition.partition_error_code.to_be_bytes());
            }
        } else {
            buffer.push(1); // No partitions
        }
        
        // topic_operations
        buffer.extend_from_slice(&self.topic_operations.to_be_bytes());
        Ok(buffer)
    }
}

#[derive(Debug, Default)]
pub struct ResDescTopicPartitionV0 {
    pub correlation_id: i32,
    pub throttle_time_ms: u32,
    pub topics:Vec<Topic>,
    pub cursor: u8,
}

impl ResDescTopicPartitionV0 {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        // Reserve 4 bytes for message_size (will be filled later)
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer.push(0);
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.push(self.topics.len() as u8 + 1); // Topic count
        for topic in &self.topics {
            let topic_bytes = topic.to_bytes()?;
            buffer.extend_from_slice(&topic_bytes);
            buffer.push(0); // Null terminator byte
        }
        buffer.push(self.cursor);
        // Add a final 0 byte to indicate the end of the topics array
        buffer.push(0);
        
        Ok(buffer)
    }

    pub fn from_request(request: &ReqDescTopicPartitionV0) -> Self {
        let mut topics: Vec<Topic> = vec![];
        for topic_name in &request.topics {
            topics.push(Topic {
                error_code: 3, // Default to no error
                topic_name: topic_name.clone(),
                topic_id: 0, // Placeholder ID
                is_internal: false,
                partitions: None, // No partitions by default
                topic_operations: 0, // Default operations
            });
        }

        ResDescTopicPartitionV0 {
            correlation_id: request.header.correlation_id,
            throttle_time_ms: 0, // Default to no throttle
            topics: topics,
            cursor: request.cursor,
        }
    }
}