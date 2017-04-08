use std::convert::From;
use super::super::IntoBytes;

#[derive(Debug, PartialEq,Default)]
pub struct BodyResReady;


impl From<Vec<u8>> for BodyResReady {
    fn from(_vec: Vec<u8>) -> BodyResReady {
        BodyResReady {}
    }
}

impl IntoBytes for BodyResReady {
    fn into_cbytes(&self) -> Vec<u8> {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use IntoBytes;

    #[test]
    fn body_res_ready_new() {
        let body :BodyResReady = Default::default();
        assert_eq!(body, BodyResReady {});
    }

    #[test]
    fn body_res_ready_into_cbytes() {
        let body = BodyResReady {};
        assert_eq!(body.into_cbytes(), vec![] as Vec<u8>);
    }

    #[test]
    fn body_res_ready_from() {
        let body = BodyResReady::from(vec![]);
        assert_eq!(body, BodyResReady {});
    }
}
