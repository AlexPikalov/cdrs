use std::io;
use std::io::Read;
use std::net;

use super::*;
use super::super::types::from_bytes;

pub fn parse_frame(mut cursor: net::TcpStream) -> io::Result<Frame> {
    // let mut cursor = io::Cursor::new(vec);
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

    return Ok(Frame {
        version: version,
        flags: vec![flag],
        opcode: opcode,
        stream: stream,
        body: body_bytes
    });
}
