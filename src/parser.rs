use std::io::{Cursor, Read};

use super::frame::*;
use super::types::from_bytes;

pub fn parse_frame(vec: Vec<u8>) -> Frame {
    let mut cursor = Cursor::new(vec);
    let mut version_bytes = [0; VERSION_LEN];
    let mut flag_bytes = [0; FLAG_LEN];
    let mut opcode_bytes = [0; OPCODE_LEN];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes =[0; LENGTH_LEN];

    // NOTE: order of reads matters
    if let Err(err) = cursor.read(&mut version_bytes) {
        error!("Parse Cassandra version error: {}", err);
        panic!(err);
    }
    if let Err(err) = cursor.read(&mut flag_bytes) {
        error!("Parse Cassandra flag error: {}", err);
        panic!(err);
    }
    if let Err(err) = cursor.read(&mut stream_bytes) {
        error!("Parse Cassandra stream error: {}", err);
        panic!(err);
    }
    if let Err(err) = cursor.read(&mut opcode_bytes) {
        error!("Parse Cassandra opcode error: {}", err);
        panic!(err);
    }
    if let Err(err) = cursor.read(&mut length_bytes) {
        error!("Parse Cassandra length error: {}", err);
        panic!(err);
    }

    let version = Version::from(version_bytes.to_vec());
    let flag = Flag::from(flag_bytes.to_vec());
    let stream = from_bytes(stream_bytes.to_vec());
    let opcode = Opcode::from(opcode_bytes.to_vec());
    let length = from_bytes(length_bytes.to_vec()) as usize;

    let mut body_bytes = Vec::new();
    if let Err(err) = cursor.read_to_end(&mut body_bytes) {
        error!("Parse Cassandra body error: {}", err);
        panic!(err);
    }
    body_bytes.truncate(length);

    return Frame {
        version: version,
        flag: flag,
        opcode: opcode,
        stream: stream,
        body: body_bytes
    }
}
