use crate::envelope::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use crate::error;
use crate::types::{from_cursor_str, serialize_str, CIntShort};
use std::collections::HashMap;
use std::io::Cursor;

const CQL_VERSION: &str = "CQL_VERSION";
const CQL_VERSION_VAL: &str = "3.0.0";
const COMPRESSION: &str = "COMPRESSION";
const DRIVER_NAME: &str = "DRIVER_NAME";
const DRIVER_VERSION: &str = "DRIVER_VERSION";

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct BodyReqStartup {
    pub map: HashMap<String, String>,
}

impl BodyReqStartup {
    pub fn new(compression: Option<String>, version: Version) -> BodyReqStartup {
        let mut map = HashMap::new();
        map.insert(CQL_VERSION.into(), CQL_VERSION_VAL.into());
        if let Some(c) = compression {
            map.insert(COMPRESSION.into(), c);
        }

        if version >= Version::V5 {
            map.insert(DRIVER_NAME.into(), "cdrs-tokio".into());
            if let Some(version) = option_env!("CARGO_PKG_VERSION") {
                map.insert(DRIVER_VERSION.into(), version.into());
            }
        }

        BodyReqStartup { map }
    }
}

impl Serialize for BodyReqStartup {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        let num = self.map.len() as CIntShort;
        num.serialize(cursor, version);

        for (key, val) in &self.map {
            serialize_str(cursor, key, version);
            serialize_str(cursor, val, version);
        }
    }
}

impl FromCursor for BodyReqStartup {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        let num = CIntShort::from_cursor(cursor, version)?;

        let mut map = HashMap::with_capacity(num as usize);
        for _ in 0..num {
            map.insert(
                from_cursor_str(cursor)?.to_string(),
                from_cursor_str(cursor)?.to_string(),
            );
        }

        Ok(BodyReqStartup { map })
    }
}

impl Envelope {
    /// Creates new envelope of type `startup`.
    pub fn new_req_startup(compression: Option<String>, version: Version) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::Startup;
        let body = BodyReqStartup::new(compression, version);

        Envelope::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            0,
            body.serialize_to_vec(version),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::envelope::{Envelope, Flags, Opcode, Version};

    #[test]
    fn new_body_req_startup_some_compression() {
        let compression = "test_compression";
        let body = BodyReqStartup::new(Some(compression.into()), Version::V4);
        assert_eq!(
            body.map.get("CQL_VERSION"),
            Some("3.0.0".to_string()).as_ref()
        );
        assert_eq!(
            body.map.get("COMPRESSION"),
            Some(compression.to_string()).as_ref()
        );
        assert_eq!(body.map.len(), 2);
    }

    #[test]
    fn new_body_req_startup_none_compression() {
        let body = BodyReqStartup::new(None, Version::V4);
        assert_eq!(
            body.map.get("CQL_VERSION"),
            Some("3.0.0".to_string()).as_ref()
        );
        assert_eq!(body.map.len(), 1);
    }

    #[test]
    fn new_req_startup() {
        let compression = Some("test_compression".to_string());
        let frame = Envelope::new_req_startup(compression, Version::V4);
        assert_eq!(frame.version, Version::V4);
        assert_eq!(frame.flags, Flags::empty());
        assert_eq!(frame.opcode, Opcode::Startup);
        assert_eq!(frame.tracing_id, None);
        assert!(frame.warnings.is_empty());
    }

    #[test]
    fn body_req_startup_from_cursor() {
        let bytes = vec![
            0, 3, 0, 11, 68, 82, 73, 86, 69, 82, 95, 78, 65, 77, 69, 0, 22, 68, 97, 116, 97, 83,
            116, 97, 120, 32, 80, 121, 116, 104, 111, 110, 32, 68, 114, 105, 118, 101, 114, 0, 14,
            68, 82, 73, 86, 69, 82, 95, 86, 69, 82, 83, 73, 79, 78, 0, 6, 51, 46, 50, 53, 46, 48,
            0, 11, 67, 81, 76, 95, 86, 69, 82, 83, 73, 79, 78, 0, 5, 51, 46, 52, 46, 53,
        ];

        let mut cursor = Cursor::new(bytes.as_slice());
        BodyReqStartup::from_cursor(&mut cursor, Version::V4).unwrap();
    }
}
