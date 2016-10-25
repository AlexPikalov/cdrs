// NOTE: for development purposes only. Will be removed soon
use std::io::prelude::*;
use std::net::TcpStream;

extern crate cdrs;
use cdrs::frame::Frame;
use cdrs::parser::parse_frame;
use cdrs::IntoBytes;
use cdrs::consistency::Consistency;
// use cadrs::frame_startup::BodyReqStartup;


fn main() {
    println!("connecting");
    let startup_message = Frame::new_req_startup(None);
    let use_query = String::from("USE loghub;");
    let q = Frame::new_req_query(use_query.clone(), Consistency::Any, None, None, None, None, None, None);
    println!(">>>>> {:?}", String::from_utf8_lossy(use_query.into_bytes().as_slice()));
    match TcpStream::connect("127.0.0.1:9042") {
        Err(err) => println!("{:?}", err),
        Ok(mut stream) => {
            println!("Ok. Starting connection");
            let vec: Vec<u8> = startup_message.into_cbytes();
            println!("Frame {:?}", vec);
            println!("Frame text {:?}", String::from_utf8(vec.clone()).unwrap());
            match stream.write(vec.as_slice()) {
                Ok(_) => {
                    println!("frame sent");
                },
                Err(err) => panic!("Error during writing into socket {}", err)
            }

            loop {
                let mut res = [0; 1024];
                match stream.read(&mut res) {
                    Ok(n) => {
                        if n > 0 {
                            // println!("Server response {:?}", String::from_utf8_lossy(&res[..]));
                            let parsed = parse_frame(res[..].to_vec());
                            println!("Parsed response {:?}", parsed);
                            match stream.write(q.into_cbytes().as_slice()) {
                                Ok(_) => {
                                    println!("frame sent use");
                                    // return;
                                },
                                Err(err) => panic!("Error during writing into socket {}", err)
                            }
                            continue;
                        } else {
                            println!("waiting...");
                        }

                    },
                    Err(err) => panic!("Error during reading from socket {}", err)
                }
            }
        }
    }
}
