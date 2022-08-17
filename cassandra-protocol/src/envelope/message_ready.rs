use crate::envelope::{FromCursor, Serialize, Version};
use crate::error;
use std::io::Cursor;

#[derive(Debug, PartialEq, Default, Ord, PartialOrd, Eq, Hash)]
pub struct BodyResReady;

impl Serialize for BodyResReady {
    #[inline(always)]
    fn serialize(&self, _cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {}
}

impl FromCursor for BodyResReady {
    #[inline(always)]
    fn from_cursor(_cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<Self> {
        Ok(BodyResReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_res_ready_new() {
        let body: BodyResReady = Default::default();
        assert_eq!(body, BodyResReady);
    }

    #[test]
    fn body_res_ready_serialize() {
        let body = BodyResReady;
        assert!(body.serialize_to_vec(Version::V4).is_empty());
    }
}
