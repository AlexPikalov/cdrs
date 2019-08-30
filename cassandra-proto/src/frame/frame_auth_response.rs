use rand;

use crate::types::CBytes;
use crate::frame::*;

#[derive(Debug)]
pub struct BodyReqAuthResponse {
    data: CBytes,
}

impl BodyReqAuthResponse {
    pub fn new(data: CBytes) -> BodyReqAuthResponse {
        BodyReqAuthResponse { data: data }
    }
}

impl IntoBytes for BodyReqAuthResponse {
    fn into_cbytes(&self) -> Vec<u8> {
        self.data.into_cbytes()
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `AuthResponse`.
    pub fn new_req_auth_response(bytes: Vec<u8>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        let stream = rand::random::<u16>();
        let opcode = Opcode::AuthResponse;
        let body = BodyReqAuthResponse::new(CBytes::new(bytes));

        Frame { version: version,
                flags: vec![flag],
                stream: stream,
                opcode: opcode,
                body: body.into_cbytes(),
                // for request frames it's always None
                tracing_id: None,
                warnings: vec![], }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CBytes;
    use crate::frame::traits::IntoBytes;

    #[test]
    fn body_req_auth_response() {
        let bytes = CBytes::new(vec![1, 2, 3]);
        let body = BodyReqAuthResponse::new(bytes);
        assert_eq!(body.into_cbytes(), vec![0, 0, 0, 3, 1, 2, 3]);
    }

    #[test]
    fn frame_body_req_auth_response() {
        let bytes = vec![1, 2, 3];
        let frame = Frame::new_req_auth_response(bytes);

        assert_eq!(frame.version, Version::Request);
        assert_eq!(frame.flags, vec![Flag::Ignore]);
        assert_eq!(frame.opcode, Opcode::AuthResponse);
        assert_eq!(frame.body, &[0, 0, 0, 3, 1, 2, 3]);
        assert_eq!(frame.tracing_id, None);
        assert_eq!(frame.warnings.len(), 0);
    }
}
