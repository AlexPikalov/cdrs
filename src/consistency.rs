#![warn(missing_docs)]
//! The module contains Rust representation of Cassandra consistency levels.
use std::convert::From;
use std::default::Default;
use std::io;

use crate::error;
use crate::frame::{FromBytes, FromCursor, IntoBytes};
use crate::types::*;

/// `Consistency` is an enum which represents Cassandra's consistency levels.
/// To find more details about each consistency level please refer to Cassandra official docs.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Consistency {
    /// A write must be written to the commit log and memtable on all replica nodes in the cluster
    /// for that partition key.	Provides the highest consistency
    /// and the lowest availability of any other level.
    Any,
    ///
    /// A write must be written to the commit log and memtable of at least one replica node.
    /// Satisfies the needs of most users because consistency requirements are not stringent.
    One,
    /// A write must be written to the commit log and memtable of at least two replica nodes.
    /// Similar to ONE.
    Two,
    /// A write must be written to the commit log and memtable of at least three replica nodes.
    /// Similar to TWO.
    Three,
    /// A write must be written to the commit log and memtable on a quorum of replica nodes.
    /// Provides strong consistency if you can tolerate some level of failure.
    Quorum,
    /// A write must be written to the commit log and memtable on all replica nodes in the cluster
    /// for that partition key.
    /// Provides the highest consistency and the lowest availability of any other level.
    All,
    /// Strong consistency. A write must be written to the commit log and memtable on a quorum
    /// of replica nodes in the same data center as thecoordinator node.
    /// Avoids latency of inter-data center communication.
    /// Used in multiple data center clusters with a rack-aware replica placement strategy,
    /// such as NetworkTopologyStrategy, and a properly configured snitch.
    /// Use to maintain consistency locally (within the single data center).
    /// Can be used with SimpleStrategy.
    LocalQuorum,
    /// Strong consistency. A write must be written to the commit log and memtable on a quorum of
    /// replica nodes in all data center.
    /// Used in multiple data center clusters to strictly maintain consistency at the same level
    /// in each data center. For example, choose this level
    /// if you want a read to fail when a data center is down and the QUORUM
    /// cannot be reached on that data center.
    EachQuorum,
    /// Achieves linearizable consistency for lightweight transactions by preventing unconditional
    /// updates.	You cannot configure this level as a normal consistency level,
    /// configured at the driver level using the consistency level field.
    /// You configure this level using the serial consistency field
    /// as part of the native protocol operation. See failure scenarios.
    Serial,
    /// Same as SERIAL but confined to the data center. A write must be written conditionally
    /// to the commit log and memtable on a quorum of replica nodes in the same data center.
    /// Same as SERIAL. Used for disaster recovery. See failure scenarios.
    LocalSerial,
    /// A write must be sent to, and successfully acknowledged by,
    /// at least one replica node in the local data center.
    /// In a multiple data center clusters, a consistency level of ONE is often desirable,
    /// but cross-DC traffic is not. LOCAL_ONE accomplishes this.
    /// For security and quality reasons, you can use this consistency level
    /// in an offline datacenter to prevent automatic connection
    /// to online nodes in other data centers if an offline node goes down.
    LocalOne,
    /// This is an error scenario either the client code doesn't support it or server is sending
    /// bad headers
    Unknown,
}

impl Default for Consistency {
    fn default() -> Consistency {
        Consistency::One
    }
}

impl IntoBytes for Consistency {
    fn into_cbytes(&self) -> Vec<u8> {
        match *self {
            Consistency::Any => to_short(0x0000),
            Consistency::One => to_short(0x0001),
            Consistency::Two => to_short(0x0002),
            Consistency::Three => to_short(0x0003),
            Consistency::Quorum => to_short(0x0004),
            Consistency::All => to_short(0x0005),
            Consistency::LocalQuorum => to_short(0x0006),
            Consistency::EachQuorum => to_short(0x0007),
            Consistency::Serial => to_short(0x0008),
            Consistency::LocalSerial => to_short(0x0009),
            Consistency::LocalOne => to_short(0x000A),
            Consistency::Unknown => to_short(0x0063),
            // giving Unknown a value of 99
        }
    }
}

impl From<i32> for Consistency {
    fn from(bytes: i32) -> Consistency {
        match bytes {
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
            _ => Consistency::Unknown,
        }
    }
}

impl FromBytes for Consistency {
    fn from_bytes(bytes: &[u8]) -> error::Result<Consistency> {
        try_from_bytes(bytes).map_err(Into::into).map(|b| match b {
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
            _ => Consistency::Unknown,
        })
    }
}

impl FromCursor for Consistency {
    fn from_cursor(mut cursor: &mut io::Cursor<&[u8]>) -> error::Result<Consistency> {
        let consistency_num = CIntShort::from_cursor(&mut cursor)? as i32;
        Ok(Consistency::from(consistency_num))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::{FromBytes, FromCursor, IntoBytes};
    use std::io::Cursor;

    #[test]
    fn test_consistency_into_cbytes() {
        assert_eq!(Consistency::Any.into_cbytes(), &[0, 0]);
        assert_eq!(Consistency::One.into_cbytes(), &[0, 1]);
        assert_eq!(Consistency::Two.into_cbytes(), &[0, 2]);
        assert_eq!(Consistency::Three.into_cbytes(), &[0, 3]);
        assert_eq!(Consistency::Quorum.into_cbytes(), &[0, 4]);
        assert_eq!(Consistency::All.into_cbytes(), &[0, 5]);
        assert_eq!(Consistency::LocalQuorum.into_cbytes(), &[0, 6]);
        assert_eq!(Consistency::EachQuorum.into_cbytes(), &[0, 7]);
        assert_eq!(Consistency::Serial.into_cbytes(), &[0, 8]);
        assert_eq!(Consistency::LocalSerial.into_cbytes(), &[0, 9]);
        assert_eq!(Consistency::LocalOne.into_cbytes(), &[0, 10]);
        assert_eq!(Consistency::Unknown.into_cbytes(), &[0, 99]);
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
        assert_eq!(Consistency::from(11), Consistency::Unknown);
    }

    #[test]
    fn test_consistency_from_bytes() {
        assert_eq!(Consistency::from_bytes(&[0, 0]).unwrap(), Consistency::Any);
        assert_eq!(Consistency::from_bytes(&[0, 1]).unwrap(), Consistency::One);
        assert_eq!(Consistency::from_bytes(&[0, 2]).unwrap(), Consistency::Two);
        assert_eq!(
            Consistency::from_bytes(&[0, 3]).unwrap(),
            Consistency::Three
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 4]).unwrap(),
            Consistency::Quorum
        );
        assert_eq!(Consistency::from_bytes(&[0, 5]).unwrap(), Consistency::All);
        assert_eq!(
            Consistency::from_bytes(&[0, 6]).unwrap(),
            Consistency::LocalQuorum
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 7]).unwrap(),
            Consistency::EachQuorum
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 8]).unwrap(),
            Consistency::Serial
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 9]).unwrap(),
            Consistency::LocalSerial
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 10]).unwrap(),
            Consistency::LocalOne
        );
        assert_eq!(
            Consistency::from_bytes(&[0, 11]).unwrap(),
            Consistency::Unknown
        );
    }

    #[test]
    fn test_consistency_from_cursor() {
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 0])).unwrap(),
            Consistency::Any
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 1])).unwrap(),
            Consistency::One
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 2])).unwrap(),
            Consistency::Two
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 3])).unwrap(),
            Consistency::Three
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 4])).unwrap(),
            Consistency::Quorum
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 5])).unwrap(),
            Consistency::All
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 6])).unwrap(),
            Consistency::LocalQuorum
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 7])).unwrap(),
            Consistency::EachQuorum
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 8])).unwrap(),
            Consistency::Serial
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 9])).unwrap(),
            Consistency::LocalSerial
        );
        assert_eq!(
            Consistency::from_cursor(&mut Cursor::new(&[0, 10])).unwrap(),
            Consistency::LocalOne
        );
    }
}
