use super::IntoBytes;
use super::FromBytes;
use super::types::*;

pub enum ResultKind {
    Void,
    Rows,
    SetKeyspace,
    Prepared,
    SchemaChange
}

impl IntoBytes for ResultKind {
    fn into_cbytes(&self) -> Vec<u8> {
        return match *self {
            ResultKind::Void => to_int(0x0001),
            ResultKind::Rows => to_int(0x0002),
            ResultKind::SetKeyspace => to_int(0x0003),
            ResultKind::Prepared => to_int(0x0004),
            ResultKind::SchemaChange => to_int(0x0005)
        }
    }
}

impl FromBytes for ResultKind {
    fn from_bytes(bytes: Vec<u8>) -> ResultKind {
        return match from_bytes(bytes.clone()) {
            0x0001 => ResultKind::Void,
            0x0002 => ResultKind::Rows,
            0x0003 => ResultKind::SetKeyspace,
            0x0004 => ResultKind::Prepared,
            0x0005 => ResultKind::SchemaChange,
            _ => {
                error!("Unexpected Cassandra result kind: {:?}", bytes);
                panic!("Unexpected Cassandra result kind: {:?}", bytes);
            }
        };
    }
}
