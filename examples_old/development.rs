extern crate cdrs;
extern crate r2d2;

use std::thread::sleep;
use std::time::Duration;

use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::connection_manager::ConnectionManager;

use r2d2::ManageConnection;

fn main() {
    let transport = TransportTcp::new("127.0.0.1:9042").expect("Failed to connect");
    let manager = ConnectionManager::new(transport, NoneAuthenticator, Compression::None);

    let mut connection = manager.connect().expect("Failed to connect");
    loop {
        let is_valid = manager.is_valid(&mut connection);
        println!("{:?}", is_valid);
        sleep(Duration::from_secs(1));
        if let Ok(_) = is_valid {
            continue;
        }

        match manager.connect() {
            Ok(conn) => {
                connection = conn;
            }
            Err(err) => println!("Failed to reconnect: {:?}", err),
        }
    }
}
