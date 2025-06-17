use anyhow::Result;
use bytes::{buf, BytesMut, Buf};


#[derive(Debug, Default)]
pub struct RequestHeaderV2 {
    pub message_size: u32,
    pub request_api_key: u16,
    pub request_api_version: u16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeaderV2 {
    pub fn from_bytes(bytes: &mut BytesMut) -> Result<Self> {
        let mut header = RequestHeaderV2::default();
        header.message_size = bytes.get_u32();
        header.request_api_key = bytes.get_u16();
        header.request_api_version = bytes.get_u16();
        header.correlation_id = bytes.get_i32();
        let client_id_len = bytes.get_u16();
        let client_id_bytes = bytes.split_to(client_id_len as usize);
        let client_id = String::from_utf8(client_id_bytes.to_vec()).ok();
        header.client_id = client_id;
        bytes.get_u8(); // Skip the null terminator byte
        Ok(header)
    }
}

#[derive(Debug, Default)]
pub struct ReqApiVersionsV4 {
    pub header: RequestHeaderV2,
    pub client_id: String,
    pub client_version: String,
}

impl ReqApiVersionsV4 {
    pub fn from_bytes(header: RequestHeaderV2, bytes: &mut BytesMut) -> Result<Self> {
        let mut req = ReqApiVersionsV4::default();
        req.header = header;
        let client_id_len = bytes.get_u16();
        let client_id_bytes = bytes.split_to(client_id_len as usize);
        req.client_id = String::from_utf8(client_id_bytes.to_vec()).unwrap();
        let client_version_len = bytes.get_u16();
        let client_version_bytes = bytes.split_to(client_version_len as usize);
        req.client_version = String::from_utf8(client_version_bytes.to_vec()).unwrap();
        Ok(req)
    }
}

#[derive(Debug, Default)]
pub struct ReqDescTopicPartitionV0 {
    pub header: RequestHeaderV2,
    pub topics: Vec<String>,
    pub res_partition_limit: u32,
    pub cursor: u8
}

impl ReqDescTopicPartitionV0 {
    pub fn from_bytes(header: RequestHeaderV2, bytes: &mut BytesMut) -> Result<Self> {
        let mut req = ReqDescTopicPartitionV0::default();
        req.header = header;
        let topic_count = bytes.get_u8() as usize - 1;
        for _ in 0..topic_count {
            let topic_len = bytes.get_u8() as usize;
            let topic_bytes = bytes.split_to(topic_len);
            let topic = String::from_utf8(topic_bytes.to_vec()).unwrap();
            bytes.get_u8(); // Skip the null terminator byte
            req.topics.push(topic);
        }
        req.res_partition_limit = bytes.get_u32();
        req.cursor = bytes.get_u8();
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use anyhow::Result;
    #[test]
    fn test_request_header_v2_from_bytes() -> Result<()> {
        let mut bytes = BytesMut::from(&[
            0, 0, 0, 20, // message_size
            0, 18, // request_api_key
            0, 2, // request_api_version
            0, 0, 0, 1, // correlation_id
            0, 5, // client_id length
            b'h', b'e', b'l', b'l', b'o', // client_id
            0, // null terminator
        ][..]);
        let header = RequestHeaderV2::from_bytes(&mut bytes)?;
        assert_eq!(header.message_size, 20);
        assert_eq!(header.request_api_key, 18);
        assert_eq!(header.request_api_version, 2);
        assert_eq!(header.correlation_id, 1);
        assert_eq!(header.client_id, Some("hello".to_string()));
        Ok(())
    }

    #[test]
    fn test_req_api_versions_from_bytes() -> Result<()> {
        let mut bytes = BytesMut::from(&[
            0, 0, 0, 30, // message_size
            0, 18, // request_api_key
            0, 2, // request_api_version
            0, 0, 0, 1, // correlation_id
            0, 5, // client_id length
            b'h', b'e', b'l', b'l', b'o', // client_id
            0, // null terminator
            0, 5, // client_id length
            b'h', b'e', b'l', b'l', b'o', 
            0, 6, // client_version length
            b'w', b'o', b'r', b'l', b'd', b'!',
            0, // null terminator
        ][..]);
        let header = RequestHeaderV2::from_bytes(&mut bytes)?;
        let req = ReqApiVersionsV4::from_bytes(header, &mut bytes)?;
        assert_eq!(req.header.message_size, 30);
        assert_eq!(req.header.request_api_key, 18);
        assert_eq!(req.header.request_api_version, 2);
        assert_eq!(req.header.correlation_id, 1);
        assert_eq!(req.header.client_id, Some("hello".to_string()));
        assert_eq!(req.client_id, "hello");
        assert_eq!(req.client_version, "world!");
        Ok(())
    }
}