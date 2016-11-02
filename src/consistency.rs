#![warn(missing_docs)]
//! The module contains Rust representation of Cassandra consistency levels.
use super::{IntoBytes};
use super::types::*;
use super::FromBytes;

/// `Consistency` is an enum which represents Cassandra's consistency levels.
/// To find more details about each consistency level please refer to Cassandra official docs.
pub enum Consistency {
    #[allow(missing_docs)]
    Any,
    #[allow(missing_docs)]
    One,
    #[allow(missing_docs)]
    Two,
    #[allow(missing_docs)]
    Three,
    #[allow(missing_docs)]
    Quorum,
    #[allow(missing_docs)]
    All,
    #[allow(missing_docs)]
    LocalQuorum,
    #[allow(missing_docs)]
    EachQuorum,
    #[allow(missing_docs)]
    Serial,
    #[allow(missing_docs)]
    LocalSerial,
    #[allow(missing_docs)]
    LocalOne
}

impl IntoBytes for Consistency {
    fn into_cbytes(&self) -> Vec<u8> {
        return match self {
            &Consistency::Any => to_short(0x0000),
            &Consistency::One => to_short(0x0001),
            &Consistency::Two => to_short(0x0002),
            &Consistency::Three => to_short(0x0003),
            &Consistency::Quorum => to_short(0x0004),
            &Consistency::All => to_short(0x0005),
            &Consistency::LocalQuorum => to_short(0x0006),
            &Consistency::EachQuorum => to_short(0x0007),
            &Consistency::Serial => to_short(0x0008),
            &Consistency::LocalSerial => to_short(0x0009),
            &Consistency::LocalOne => to_short(0x000A)
        };
    }
}

impl FromBytes for Consistency {
    fn from_bytes(bytes: Vec<u8>) -> Consistency {
        return match from_bytes(bytes.clone()) {
            0x0000 => Consistency::Any,
            0x0001 => Consistency::One,
            0x0002 => Consistency::Two,
            0x0003 => Consistency::Three,
            0x0004 => Consistency::Quorum,
            0x0005 => Consistency::All,
            0x0006 => Consistency::LocalQuorum,
            0x0007 => Consistency::EachQuorum,
            0x0008 => Consistency::Serial,
            0x0009 => Consistency::LocalSerial,
            0x000A => Consistency::LocalOne,
            _ => unreachable!()
        };
    }
}
