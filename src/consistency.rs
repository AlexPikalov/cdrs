#![warn(missing_docs)]
//! The module contains Rust representation of Cassandra consistency levels.
use std::io;
use std::convert::From;
use std::default::Default;
use super::{IntoBytes, FromCursor};
use super::types::*;
use super::FromBytes;

/// `Consistency` is an enum which represents Cassandra's consistency levels.
/// To find more details about each consistency level please refer to Cassandra official docs.
#[derive(Debug, PartialEq, Clone)]
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

impl Default for Consistency {
    fn default() -> Consistency {
        Consistency::One
    }
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

impl From<i32> for Consistency {
    fn from(bytes: i32) -> Consistency {
        return match bytes {
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

impl FromCursor for Consistency {
    fn from_cursor(mut cursor: &mut io::Cursor<Vec<u8>>) -> Consistency {
        let consistency_num = CIntShort::from_cursor(&mut cursor) as i32;
        return Consistency::from(consistency_num);
    }
}



#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::{IntoBytes, FromBytes, FromCursor};
    use super::Consistency;

    #[test]
    fn test_consistency_into_cbytes() {
        assert_eq!(Consistency::Any.into_cbytes(), vec![0, 0]);
        assert_eq!(Consistency::One.into_cbytes(), vec![0, 1]);
        assert_eq!(Consistency::Two.into_cbytes(), vec![0, 2]);
        assert_eq!(Consistency::Three.into_cbytes(), vec![0, 3]);
        assert_eq!(Consistency::Quorum.into_cbytes(), vec![0, 4]);
        assert_eq!(Consistency::All.into_cbytes(), vec![0, 5]);
        assert_eq!(Consistency::LocalQuorum.into_cbytes(), vec![0, 6]);
        assert_eq!(Consistency::EachQuorum.into_cbytes(), vec![0, 7]);
        assert_eq!(Consistency::Serial.into_cbytes(), vec![0, 8]);
        assert_eq!(Consistency::LocalSerial.into_cbytes(), vec![0, 9]);
        assert_eq!(Consistency::LocalOne.into_cbytes(), vec![0, 10]);
    }

    #[test]
    fn test_consistency_from() {
        assert_eq!(Consistency::from(0), Consistency::Any);
        assert_eq!(Consistency::from(1), Consistency::One);
        assert_eq!(Consistency::from(2), Consistency::Two);
        assert_eq!(Consistency::from(3), Consistency::Three);
        assert_eq!(Consistency::from(4), Consistency::Quorum);
        assert_eq!(Consistency::from(5), Consistency::All);
        assert_eq!(Consistency::from(6), Consistency::LocalQuorum);
        assert_eq!(Consistency::from(7), Consistency::EachQuorum);
        assert_eq!(Consistency::from(8), Consistency::Serial);
        assert_eq!(Consistency::from(9), Consistency::LocalSerial);
        assert_eq!(Consistency::from(10), Consistency::LocalOne);
    }

    #[test]
    fn test_consistency_from_bytes() {
        assert_eq!(Consistency::from_bytes(vec![0, 0]), Consistency::Any);
        assert_eq!(Consistency::from_bytes(vec![0, 1]), Consistency::One);
        assert_eq!(Consistency::from_bytes(vec![0, 2]), Consistency::Two);
        assert_eq!(Consistency::from_bytes(vec![0, 3]), Consistency::Three);
        assert_eq!(Consistency::from_bytes(vec![0, 4]), Consistency::Quorum);
        assert_eq!(Consistency::from_bytes(vec![0, 5]), Consistency::All);
        assert_eq!(Consistency::from_bytes(vec![0, 6]), Consistency::LocalQuorum);
        assert_eq!(Consistency::from_bytes(vec![0, 7]), Consistency::EachQuorum);
        assert_eq!(Consistency::from_bytes(vec![0, 8]), Consistency::Serial);
        assert_eq!(Consistency::from_bytes(vec![0, 9]), Consistency::LocalSerial);
        assert_eq!(Consistency::from_bytes(vec![0, 10]), Consistency::LocalOne);
    }

    #[test]
    fn test_consistency_from_cursor() {
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 0])), Consistency::Any);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 1])), Consistency::One);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 2])), Consistency::Two);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 3])), Consistency::Three);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 4])), Consistency::Quorum);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 5])), Consistency::All);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 6])), Consistency::LocalQuorum);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 7])), Consistency::EachQuorum);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 8])), Consistency::Serial);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 9])), Consistency::LocalSerial);
        assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 10])), Consistency::LocalOne);
    }

}
