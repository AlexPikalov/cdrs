use types::CBytes;
use IntoBytes;
use frame::*;

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
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::AuthResponse;
        let body = BodyReqAuthResponse::new(CBytes::new(bytes));

        Frame {
            version: version,
            flags: vec![flag],
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![],
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_req_auth_response() {
        let few_bytes: Vec<u8> = vec![0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let  data = CBytes::new(few_bytes);
        let body = BodyReqAuthResponse::new(data);
        assert_eq!(body.data.into_plain(), vec![0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn body_req_auth_frame() {
        let few_bytes: Vec<u8> = vec![0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let frame = Frame::new_req_auth_response(few_bytes);
        assert_eq!(frame.version,Version::Request);
        assert_eq!(frame.flags,vec![Flag::Ignore]);
        assert_eq!(frame.stream,0);
        assert_eq!(frame.opcode,Opcode::AuthResponse);
        assert_eq!(frame.tracing_id,None);
        //assert_eq!(frame.warnings,vec![]);
        assert_eq!(frame.body,vec![0, 0, 0, 14,0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}

