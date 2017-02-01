use cdrs::AsByte;
use cdrs::frame::{Version, Flag, Opcode};

mod events;

#[test]
fn test_frame_version_as_byte() {
    let request_version = Version::Request;
    assert_eq!(request_version.as_byte(), 0x04);
    let response_version = Version::Response;
    assert_eq!(response_version.as_byte(), 0x84);
}

#[test]
fn test_frame_version_from() {
    let request: Vec<u8> = vec![0x04];
    assert_eq!(Version::from(request), Version::Request);
    let response: Vec<u8> = vec![0x84];
    assert_eq!(Version::from(response), Version::Response);
}

#[test]
fn test_flag_from() {
    assert_eq!(Flag::from(0x01 as u8), Flag::Compression);
    assert_eq!(Flag::from(0x02 as u8), Flag::Tracing);
    assert_eq!(Flag::from(0x04 as u8), Flag::CustomPayload);
    assert_eq!(Flag::from(0x08 as u8), Flag::Warning);
    // rest should be interpreted as Ignore
    assert_eq!(Flag::from(0x10 as u8), Flag::Ignore);
    assert_eq!(Flag::from(0x31 as u8), Flag::Ignore);
}

#[test]
fn test_flag_as_byte() {
    assert_eq!(Flag::Compression.as_byte(), 0x01);
    assert_eq!(Flag::Tracing.as_byte(), 0x02);
    assert_eq!(Flag::CustomPayload.as_byte(), 0x04);
    assert_eq!(Flag::Warning.as_byte(), 0x08);
}

#[test]
fn test_flag_has_x() {
    assert!(Flag::has_compression(0x01));
    assert!(!Flag::has_compression(0x02));

    assert!(Flag::has_tracing(0x02));
    assert!(!Flag::has_tracing(0x01));

    assert!(Flag::has_custom_payload(0x04));
    assert!(!Flag::has_custom_payload(0x02));

    assert!(Flag::has_warning(0x08));
    assert!(!Flag::has_warning(0x01));
}

#[test]
fn test_flag_many_to_cbytes() {
    let all = vec![Flag::Compression, Flag::Tracing, Flag::CustomPayload, Flag::Warning];
    assert_eq!(Flag::many_to_cbytes(&all), 1 | 2 | 4 | 8);
    let some = vec![Flag::Compression, Flag::Warning];
    assert_eq!(Flag::many_to_cbytes(&some), 1 | 8);
    let one = vec![Flag::Compression];
    assert_eq!(Flag::many_to_cbytes(&one), 1);
}

#[test]
fn test_flag_get_collection() {
    let all = vec![Flag::Compression, Flag::Tracing, Flag::CustomPayload, Flag::Warning];
    assert_eq!(Flag::get_collection(1 | 2 | 4 | 8), all);
    let some = vec![Flag::Compression, Flag::Warning];
    assert_eq!(Flag::get_collection(1 | 8), some);
    let one = vec![Flag::Compression];
    assert_eq!(Flag::get_collection(1), one);
}

#[test]
fn test_opcode_as_byte() {
    assert_eq!(Opcode::Error.as_byte(), 0x00);
    assert_eq!(Opcode::Startup.as_byte(), 0x01);
    assert_eq!(Opcode::Ready.as_byte(), 0x02);
    assert_eq!(Opcode::Authenticate.as_byte(), 0x03);
    assert_eq!(Opcode::Options.as_byte(), 0x05);
    assert_eq!(Opcode::Supported.as_byte(), 0x06);
    assert_eq!(Opcode::Query.as_byte(), 0x07);
    assert_eq!(Opcode::Result.as_byte(), 0x08);
    assert_eq!(Opcode::Prepare.as_byte(), 0x09);
    assert_eq!(Opcode::Execute.as_byte(), 0x0A);
    assert_eq!(Opcode::Register.as_byte(), 0x0B);
    assert_eq!(Opcode::Event.as_byte(), 0x0C);
    assert_eq!(Opcode::Batch.as_byte(), 0x0D);
    assert_eq!(Opcode::AuthChallenge.as_byte(), 0x0E);
    assert_eq!(Opcode::AuthResponse.as_byte(), 0x0F);
    assert_eq!(Opcode::AuthSuccess.as_byte(), 0x10);
}

#[test]
fn test_opcode_from() {
    assert_eq!(Opcode::from(0x00), Opcode::Error);
    assert_eq!(Opcode::from(0x01), Opcode::Startup);
    assert_eq!(Opcode::from(0x02), Opcode::Ready);
    assert_eq!(Opcode::from(0x03), Opcode::Authenticate);
    assert_eq!(Opcode::from(0x05), Opcode::Options);
    assert_eq!(Opcode::from(0x06), Opcode::Supported);
    assert_eq!(Opcode::from(0x07), Opcode::Query);
    assert_eq!(Opcode::from(0x08), Opcode::Result);
    assert_eq!(Opcode::from(0x09), Opcode::Prepare);
    assert_eq!(Opcode::from(0x0A), Opcode::Execute);
    assert_eq!(Opcode::from(0x0B), Opcode::Register);
    assert_eq!(Opcode::from(0x0C), Opcode::Event);
    assert_eq!(Opcode::from(0x0D), Opcode::Batch);
    assert_eq!(Opcode::from(0x0E), Opcode::AuthChallenge);
    assert_eq!(Opcode::from(0x0F), Opcode::AuthResponse);
    assert_eq!(Opcode::from(0x10), Opcode::AuthSuccess);
}
