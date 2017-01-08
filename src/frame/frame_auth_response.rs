use types::CBytes;
use IntoBytes;
use frame::*;

#[derive(Debug)]
pub struct BodyReqAuthResponse {
    data: CBytes
}

impl BodyReqAuthResponse {
    pub fn new(data: CBytes) -> BodyReqAuthResponse {
        return BodyReqAuthResponse { data: data };
    }
}

impl IntoBytes for BodyReqAuthResponse {
    fn into_cbytes(&self) -> Vec<u8> {
        return self.data.into_cbytes();
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `AuthResponse`.
    pub fn new_req_auth_response(bytes: Vec<u8>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::AuthResponse;
        let body = BodyReqAuthResponse::new(CBytes::new(bytes));

        return Frame {
            version: version,
            flags: vec![flag],
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![]
        };
    }
}
