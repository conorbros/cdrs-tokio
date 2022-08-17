use cassandra_protocol::compression::Compression;
use cassandra_protocol::envelope::message_response::ResponseBody;
use cassandra_protocol::envelope::{
    Direction, Envelope, Flags, Opcode, Version, LENGTH_LEN, STREAM_LEN,
};
use cassandra_protocol::error;
use cassandra_protocol::types::data_serialization_types::decode_timeuuid;
use cassandra_protocol::types::{
    from_cursor_string_list, try_i16_from_bytes, try_i32_from_bytes, UUID_LEN,
};
use std::convert::TryFrom;
use std::io::Cursor;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;

async fn parse_raw_envelope<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
) -> error::Result<Envelope> {
    let mut version_bytes = [0; Version::BYTE_LENGTH];
    let mut flag_bytes = [0; Flags::BYTE_LENGTH];
    let mut opcode_bytes = [0; Opcode::BYTE_LENGTH];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes = [0; LENGTH_LEN];

    // NOTE: order of reads matters
    cursor.read_exact(&mut version_bytes).await?;
    cursor.read_exact(&mut flag_bytes).await?;
    cursor.read_exact(&mut stream_bytes).await?;
    cursor.read_exact(&mut opcode_bytes).await?;
    cursor.read_exact(&mut length_bytes).await?;

    let version = Version::try_from(version_bytes[0])?;
    let direction = Direction::from(version_bytes[0]);
    let flags = Flags::from_bits_truncate(flag_bytes[0]);
    let stream_id = try_i16_from_bytes(&stream_bytes)?;
    let opcode = Opcode::try_from(opcode_bytes[0])?;
    let length = try_i32_from_bytes(&length_bytes)? as usize;

    let mut body_bytes = vec![0; length];

    cursor.read_exact(&mut body_bytes).await?;

    let full_body = if flags.contains(Flags::COMPRESSION) {
        compressor.decode(body_bytes)?
    } else {
        Compression::None.decode(body_bytes)?
    };

    let body_len = full_body.len();

    // Use cursor to get tracing id, warnings and actual body
    let mut body_cursor = Cursor::new(full_body.as_slice());

    let tracing_id = if flags.contains(Flags::TRACING) {
        let mut tracing_bytes = [0; UUID_LEN];
        std::io::Read::read_exact(&mut body_cursor, &mut tracing_bytes)?;

        decode_timeuuid(&tracing_bytes).ok()
    } else {
        None
    };

    let warnings = if flags.contains(Flags::WARNING) {
        from_cursor_string_list(&mut body_cursor)?
    } else {
        vec![]
    };

    let mut body = Vec::with_capacity(body_len - body_cursor.position() as usize);

    std::io::Read::read_to_end(&mut body_cursor, &mut body)?;

    let envelope = Envelope {
        version,
        direction,
        flags,
        opcode,
        stream_id,
        body,
        tracing_id,
        warnings,
    };

    Ok(envelope)
}

pub async fn parse_envelope<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
    addr: SocketAddr,
) -> error::Result<Envelope> {
    let envelope = parse_raw_envelope(cursor, compressor).await?;
    convert_envelope_into_result(envelope, addr)
}

pub(crate) fn convert_envelope_into_result(
    envelope: Envelope,
    addr: SocketAddr,
) -> error::Result<Envelope> {
    match envelope.opcode {
        Opcode::Error => envelope.response_body().and_then(|err| match err {
            ResponseBody::Error(err) => Err(error::Error::Server { body: err, addr }),
            _ => unreachable!(),
        }),
        _ => Ok(envelope),
    }
}
