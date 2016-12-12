use std::io::Read;
use std::net;
use compression::Compression;
use frame::frame_response::ResponseBody;

use super::*;
use super::super::types::from_bytes;
use error;

pub fn parse_frame(mut cursor: net::TcpStream, compressor: &Compression) -> error::Result<Frame> {
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
    let flag = Flag::from(flag_bytes[0]);
    let stream = from_bytes(stream_bytes.to_vec());
    let opcode = Opcode::from(opcode_bytes[0]);
    let length = from_bytes(length_bytes.to_vec()) as usize;

    let mut body_bytes = Vec::with_capacity(length);
    unsafe {
        body_bytes.set_len(length);
    }
    try!(cursor.read_exact(&mut body_bytes));

    let body = if flag == Flag::Compression {
        try!(compressor.decode(body_bytes))
    } else {
        try!(Compression::None.decode(body_bytes))
    };

    let frame = Frame {
        version: version,
        flags: vec![flag],
        opcode: opcode,
        stream: stream,
        body: body
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
