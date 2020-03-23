use bb8;
use std::io::Cursor;
use std::ops::Deref;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;

use super::*;
use crate::compression::Compression;
use crate::error;
use crate::frame::frame_response::ResponseBody;
use crate::frame::FromCursor;
use crate::transport::CDRSTransport;
use crate::types::data_serialization_types::decode_timeuuid;
use crate::types::{from_bytes, from_u16_bytes, CStringList, UUID_LEN};

pub async fn from_connection<M, T>(
    conn: &bb8::PooledConnection<'_, M>,
    compressor: &Compression,
) -> error::Result<Frame>
where
    T: CDRSTransport + Unpin + 'static,
    M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
{
    parse_frame(conn.deref(), compressor).await
}

pub async fn parse_frame<T>(cursor_cell: &Mutex<T>, compressor: &Compression) -> error::Result<Frame> where T: AsyncRead + Unpin {
    let mut version_bytes = [0; Version::BYTE_LENGTH];
    let mut flag_bytes = [0; Flag::BYTE_LENGTH];
    let mut opcode_bytes = [0; Opcode::BYTE_LENGTH];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes = [0; LENGTH_LEN];
    let mut cursor = cursor_cell.lock().await;

    // NOTE: order of reads matters
    cursor.read_exact(&mut version_bytes).await?;
    cursor.read_exact(&mut flag_bytes).await?;
    cursor.read_exact(&mut stream_bytes).await?;
    cursor.read_exact(&mut opcode_bytes).await?;
    cursor.read_exact(&mut length_bytes).await?;

    let version = Version::from(version_bytes.to_vec());
    let flags = Flag::get_collection(flag_bytes[0]);
    let stream = from_u16_bytes(&stream_bytes);
    let opcode = Opcode::from(opcode_bytes[0]);
    let length = from_bytes(&length_bytes) as usize;

    let mut body_bytes = Vec::with_capacity(length);
    unsafe {
        body_bytes.set_len(length);
    }

    cursor.read_exact(&mut body_bytes).await?;

    let full_body = if flags.iter().any(|flag| flag == &Flag::Compression) {
        compressor.decode(body_bytes)?
    } else {
        Compression::None.decode(body_bytes)?
    };

    // Use cursor to get tracing id, warnings and actual body
    let mut body_cursor = Cursor::new(full_body.as_slice());

    let tracing_id = if flags.iter().any(|flag| flag == &Flag::Tracing) {
        let mut tracing_bytes = Vec::with_capacity(UUID_LEN);
        unsafe {
            tracing_bytes.set_len(UUID_LEN);
        }
        std::io::Read::read_exact(&mut body_cursor, &mut tracing_bytes)?;

        decode_timeuuid(tracing_bytes.as_slice()).ok()
    } else {
        None
    };

    let warnings = if flags.iter().any(|flag| flag == &Flag::Warning) {
        CStringList::from_cursor(&mut body_cursor)?.into_plain()
    } else {
        vec![]
    };

    let mut body = vec![];

    std::io::Read::read_to_end(&mut body_cursor, &mut body)?;

    let frame = Frame {
        version: version,
        flags: flags,
        opcode: opcode,
        stream: stream,
        body: body,
        tracing_id: tracing_id,
        warnings: warnings,
    };

    convert_frame_into_result(frame)
}

fn convert_frame_into_result(frame: Frame) -> error::Result<Frame> {
    match frame.opcode {
        Opcode::Error => frame.get_body().and_then(|err| match err {
            ResponseBody::Error(err) => Err(error::Error::Server(err)),
            _ => unreachable!(),
        }),
        _ => Ok(frame),
    }
}
