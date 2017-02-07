use std::io::{Read, Cursor};

use FromCursor;
use compression::Compression;
use frame::frame_response::ResponseBody;
use super::*;
use types::{from_bytes, UUID_LEN, CStringList};
use types::data_serialization_types::decode_timeuuid;
use error;

pub fn parse_frame(mut cursor: &mut Read, compressor: &Compression) -> error::Result<Frame> {
    let mut version_bytes = [0; VERSION_LEN];
    let mut flag_bytes = [0; FLAG_LEN];
    let mut opcode_bytes = [0; OPCODE_LEN];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes =[0; LENGTH_LEN];

    // NOTE: order of reads matters
    try!(cursor.read(&mut version_bytes));
    try!(cursor.read(&mut flag_bytes));
    try!(cursor.read(&mut stream_bytes));
    try!(cursor.read(&mut opcode_bytes));
    try!(cursor.read(&mut length_bytes));

    let version = Version::from(version_bytes.to_vec());
    let flags = Flag::get_collection(flag_bytes[0]);
    let stream = from_bytes(stream_bytes.to_vec());
    let opcode = Opcode::from(opcode_bytes[0]);
    let length = from_bytes(length_bytes.to_vec()) as usize;

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
    let mut body_cursor = Cursor::new(full_body);

    let tracing_id = if flags.iter().any(|flag| flag == &Flag::Tracing) {
        let mut tracing_bytes = Vec::with_capacity(UUID_LEN);
        unsafe {
            tracing_bytes.set_len(UUID_LEN);
        }
        try!(body_cursor.read_exact(&mut tracing_bytes));

        decode_timeuuid(tracing_bytes).ok()
    } else {
        None
    };

    let warnings = if flags.iter().any(|flag| flag == &Flag::Warning) {
        CStringList::from_cursor(&mut body_cursor).into_plain()
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
        warnings: warnings
    };

    return conver_frame_into_result(frame);
}

fn conver_frame_into_result(frame: Frame) -> error::Result<Frame> {
    match frame.opcode {
        Opcode::Error => match frame.get_body() {
            ResponseBody::Error(err) => Err(error::Error::Server(err)),
            _ => unreachable!()
        },
        _ => Ok(frame)
    }
}
