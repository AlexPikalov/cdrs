use crate::frame::*;
use crate::types::CBytes;

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
    pub fn new_req_auth_response(token_bytes: CBytes) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        let opcode = Opcode::AuthResponse;
        let body = BodyReqAuthResponse::new(token_bytes);

        Frame::new(version, vec![flag], opcode, body.into_cbytes(), None, vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::IntoBytes;
    use crate::types::CBytes;

    #[test]
    fn body_req_auth_response() {
        let bytes = CBytes::new(vec![1, 2, 3]);
        let body = BodyReqAuthResponse::new(bytes);
        assert_eq!(body.into_cbytes(), vec![0, 0, 0, 3, 1, 2, 3]);
    }

    #[test]
    fn frame_body_req_auth_response() {
        let bytes = vec![1, 2, 3];
        let frame = Frame::new_req_auth_response(CBytes::new(bytes));

        assert_eq!(frame.version, Version::Request);
        assert_eq!(frame.flags, vec![Flag::Ignore]);
        assert_eq!(frame.opcode, Opcode::AuthResponse);
        assert_eq!(frame.body, &[0, 0, 0, 3, 1, 2, 3]);
        assert_eq!(frame.tracing_id, None);
        assert_eq!(frame.warnings.len(), 0);
    }
}
