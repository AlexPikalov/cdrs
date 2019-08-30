use std::io::Cursor;

use crate::frame::FromCursor;
use crate::error;
use crate::frame::events::ServerEvent;

#[derive(Debug)]
pub struct BodyResEvent {
    pub event: ServerEvent,
}

impl FromCursor for BodyResEvent {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResEvent> {
        let event = ServerEvent::from_cursor(&mut cursor)?;

        Ok(BodyResEvent { event: event })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use crate::frame::traits::FromCursor;
    use crate::frame::events::*;

    #[test]
    fn body_res_event() {
        let bytes = [// TOPOLOGY_CHANGE
                     0,
                     15,
                     84,
                     79,
                     80,
                     79,
                     76,
                     79,
                     71,
                     89,
                     95,
                     67,
                     72,
                     65,
                     78,
                     71,
                     69,
                     // NEW_NODE
                     0,
                     8,
                     78,
                     69,
                     87,
                     95,
                     78,
                     79,
                     68,
                     69,
                     // inet - 127.0.0.1:1
                     0,
                     4,
                     127,
                     0,
                     0,
                     1,
                     0,
                     0,
                     0,
                     1];
        let mut cursor: Cursor<&[u8]> = Cursor::new(&bytes);
        let event = BodyResEvent::from_cursor(&mut cursor).unwrap().event;

        match event {
            ServerEvent::TopologyChange(ref tc) => {
                assert_eq!(tc.change_type, TopologyChangeType::NewNode);
                assert_eq!(format!("{:?}", tc.addr.addr), "V4(127.0.0.1:1)");
            }
            _ => panic!("should be topology change event"),
        }
    }
}
