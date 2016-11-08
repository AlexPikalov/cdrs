use std::io;
use std::io::{Cursor, Read};
use futures::{done, Future, BoxFuture};
use tokio_core::net::TcpStream;
use tokio_core::io::read_exact;

use super::frame::*;
use super::types::from_bytes;

pub fn parse_frame_from_future(tcp_stream: TcpStream) -> BoxFuture<Frame, io::Error> {
    let frame_future = read_exact(tcp_stream, [0; VERSION_LEN])
            // parse version
            .and_then(|(stream, version_bytes)| {
                println!("print parse version");
                let parsed_version = Version::from(version_bytes.to_vec());
                return Ok((stream, parsed_version));
            })
            // parse flag
            .and_then(|(_stream, _version)| {
                println!("print parse flag 1");
                return read_exact(_stream, [0; FLAG_LEN])
                    .and_then(|(stream, flag_bytes)| {
                        println!("print parse flag 2");
                        let flag = Flag::from(flag_bytes.to_vec());
                        return Ok((stream, _version, flag));
                    });
            })
            // parse stream
            .and_then(|(_stream, _version, _flag)| {
                println!("print parse stream 1");
                return read_exact(_stream, [0; STREAM_LEN])
                    .and_then(|(stream, stream_bytes)| {
                        println!("print parse stream 2");
                        let stream_code = Box::new(from_bytes(stream_bytes.to_vec()) as usize);
                        return Ok((stream, _version, _flag, stream_code));
                    });
            })
            // parse opcode
            .and_then(|(_stream, _version, _flag, _stream_code)| {
                println!("print parse opcode 1");
                return read_exact(_stream, [0; OPCODE_LEN])
                    .and_then(|(stream, opcode_bytes)| {
                        println!("print parse opcode 2");
                        let opcode = Opcode::from(opcode_bytes.to_vec());
                        return Ok((stream, _version, _flag, _stream_code, opcode));
                    });
            })
            // parse length
            .and_then(|(_stream, _version, _flag, _stream_code, _opcode)| {
                println!("print parse length 1");
                return read_exact(_stream, [0; LENGTH_LEN])
                    .and_then(|(stream, length_bytes)| {
                        println!("print parse length 2");
                        let length = Box::new(from_bytes(length_bytes.to_vec()) as usize);
                        return Ok((stream, _version, _flag, _stream_code, _opcode, length));
                    });
            })
            // parse body
            // if body is empty the thread hangs waiting for bytes to be read event not needing them
            // as in case of an empty body
            .and_then(|(_stream, _version, _flag, _stream_code, _opcode, _length)| {
                let l = *_length.clone();

                if l == 0 {
                    return done::<Frame, io::Error>(Ok(Frame {
                        version: _version,
                        flag: _flag,
                        opcode: _opcode,
                        stream: *_stream_code as u64,
                        body: vec![]
                    })).boxed();
                } else {
                    println!("print parse body 1 <<{}>>", _length);
                    let mut buff: Vec<u8> = Vec::with_capacity(l);
                    unsafe {
                        buff.set_len(l);
                    }

                    return read_exact(_stream, buff)
                        .and_then(|(stream, body)| {
                            println!("print parse body 2");
                            return Ok((stream, _version, _flag, _stream_code, _opcode, _length, body));
                        })
                        .map(|(_, _version, _flag, _stream_code, _opcode, _, _body)| {
                            return done::<Frame, io::Error>(Ok(Frame {
                                version: _version,
                                flag: _flag,
                                opcode: _opcode,
                                stream: *_stream_code as u64,
                                body: _body
                            }));
                        })
                        .flatten()
                        .boxed();
                }
            });

        return frame_future.boxed();
}

//
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
