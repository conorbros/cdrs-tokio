use crate::envelope::message_auth_response::BodyReqAuthResponse;
use crate::envelope::message_batch::BodyReqBatch;
use crate::envelope::message_execute::BodyReqExecuteOwned;
use crate::envelope::message_options::BodyReqOptions;
use crate::envelope::message_prepare::BodyReqPrepare;
use crate::envelope::message_query::BodyReqQuery;
use crate::envelope::message_register::BodyReqRegister;
use crate::envelope::message_startup::BodyReqStartup;
use crate::envelope::{FromCursor, Opcode, Serialize, Version};
use crate::{error, Error};
use std::io::Cursor;

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RequestBody {
    Startup(BodyReqStartup),
    Options(BodyReqOptions),
    Query(BodyReqQuery),
    Prepare(BodyReqPrepare),
    Execute(BodyReqExecuteOwned),
    Register(BodyReqRegister),
    Batch(BodyReqBatch),
    AuthResponse(BodyReqAuthResponse),
}

impl Serialize for RequestBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            RequestBody::Query(body) => body.serialize(cursor, version),
            RequestBody::Startup(body) => body.serialize(cursor, version),
            RequestBody::Options(body) => body.serialize(cursor, version),
            RequestBody::Prepare(body) => body.serialize(cursor, version),
            RequestBody::Execute(body) => body.serialize(cursor, version),
            RequestBody::Register(body) => body.serialize(cursor, version),
            RequestBody::Batch(body) => body.serialize(cursor, version),
            RequestBody::AuthResponse(body) => body.serialize(cursor, version),
        }
    }
}

impl RequestBody {
    pub fn try_from(
        bytes: &[u8],
        response_type: Opcode,
        version: Version,
    ) -> error::Result<RequestBody> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        match response_type {
            Opcode::Startup => {
                BodyReqStartup::from_cursor(&mut cursor, version).map(RequestBody::Startup)
            }
            Opcode::Options => {
                BodyReqOptions::from_cursor(&mut cursor, version).map(RequestBody::Options)
            }
            Opcode::Query => {
                BodyReqQuery::from_cursor(&mut cursor, version).map(RequestBody::Query)
            }
            Opcode::Prepare => {
                BodyReqPrepare::from_cursor(&mut cursor, version).map(RequestBody::Prepare)
            }
            Opcode::Execute => {
                BodyReqExecuteOwned::from_cursor(&mut cursor, version).map(RequestBody::Execute)
            }
            Opcode::Register => {
                BodyReqRegister::from_cursor(&mut cursor, version).map(RequestBody::Register)
            }
            Opcode::Batch => {
                BodyReqBatch::from_cursor(&mut cursor, version).map(RequestBody::Batch)
            }
            Opcode::AuthResponse => BodyReqAuthResponse::from_cursor(&mut cursor, version)
                .map(RequestBody::AuthResponse),
            _ => Err(Error::NonRequestOpcode(response_type)),
        }
    }
}
