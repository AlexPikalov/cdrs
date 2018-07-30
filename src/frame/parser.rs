use std::cell::RefCell;
use std::io::{Cursor, Read};

use super::*;
use compression::Compression;
use error;
use frame::frame_response::ResponseBody;
use frame::FromCursor;
use types::data_serialization_types::decode_timeuuid;
use types::{from_bytes, from_u16_bytes, CStringList, UUID_LEN};

pub fn parse_frame(cursor_cell: &RefCell<Read>, compressor: &Compression) -> error::Result<Frame> {
    let mut version_bytes = [0; Version::BYTE_LENGTH];
    let mut flag_bytes = [0; Flag::BYTE_LENGTH];
    let mut opcode_bytes = [0; Opcode::BYTE_LENGTH];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes = [0; LENGTH_LEN];
    let mut cursor = cursor_cell.borrow_mut();

    // NOTE: order of reads matters
    try!(cursor.read_exact(&mut version_bytes));
    try!(cursor.read_exact(&mut flag_bytes));
    try!(cursor.read_exact(&mut stream_bytes));
    try!(cursor.read_exact(&mut opcode_bytes));
    try!(cursor.read_exact(&mut length_bytes));

    let version = Version::from(version_bytes.to_vec());
    let flags = Flag::get_collection(flag_bytes[0]);
    let stream = from_u16_bytes(&stream_bytes);
    let opcode = Opcode::from(opcode_bytes[0]);
    let length = from_bytes(&length_bytes) as usize;

    let mut body_bytes = Vec::with_capacity(length);
    unsafe {
        body_bytes.set_len(length);
    }

    try!(cursor.read_exact(&mut body_bytes));

    let full_body = if flags.iter().any(|flag| flag == &Flag::Compression) {
        try!(compressor.decode(body_bytes))
    } else {
        try!(Compression::None.decode(body_bytes))
    };

    // Use cursor to get tracing id, warnings and actual body
    let mut body_cursor = Cursor::new(full_body.as_slice());

    let tracing_id = if flags.iter().any(|flag| flag == &Flag::Tracing) {
        let mut tracing_bytes = Vec::with_capacity(UUID_LEN);
        unsafe {
            tracing_bytes.set_len(UUID_LEN);
        }
        try!(body_cursor.read_exact(&mut tracing_bytes));

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

    try!(body_cursor.read_to_end(&mut body));

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
