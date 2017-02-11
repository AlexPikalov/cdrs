use FromCursor;
use std::io::Cursor;
use frame::events::ServerEvent;

#[derive(Debug)]
pub struct BodyResEvent {
    pub event: ServerEvent,
}

impl FromCursor for BodyResEvent {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> BodyResEvent {
        let event = ServerEvent::from_cursor(&mut cursor);
        BodyResEvent { event: event }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use FromCursor;
    use frame::events::*;

    #[test]
    fn body_res_event() {
        let bytes = vec![// TOPOLOGY_CHANGE
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
        let mut cursor = Cursor::new(bytes);
        let event = BodyResEvent::from_cursor(&mut cursor).event;

        match event {
            ServerEvent::TopologyChange(ref tc) => {
                assert_eq!(tc.change_type, TopologyChangeType::NewNode);
                assert_eq!(format!("{:?}", tc.addr.addr), "V4(127.0.0.1:1)");
            }
            _ => panic!("should be topology change event"),
        }
    }
}
