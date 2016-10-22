use std::io::prelude::*;
use std::net::TcpStream;

extern crate cdrs;
use cdrs::frame::Frame;
use cdrs::parser::parse_frame;
use cdrs::IntoBytes;
// use cadrs::frame_startup::BodyReqStartup;


fn main() {
    println!("connecting");
    let startup_message = Frame::new_req_startup(None);
    match TcpStream::connect("127.0.0.1:9042") {
        Err(err) => println!("{:?}", err),
        Ok(mut stream) => {
            println!("Ok. Starting connection");
            let vec: Vec<u8> = startup_message.into_bytes();
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
                            // println!("Server response {:?}", &res[..]);
                            let parsed = parse_frame(res[..].to_vec());
                            println!("Parsed response {:?}", parsed);
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
