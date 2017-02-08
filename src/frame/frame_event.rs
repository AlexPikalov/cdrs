use FromCursor;
use std::io::Cursor;
use frame::events::ServerEvent;

#[derive(Debug)]
pub struct BodyResEvent {
    pub event: ServerEvent,
}

impl FromCursor for BodyResEvent {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResEvent {
        let event = ServerEvent::from_cursor(&mut cursor);
        BodyResEvent { event: event }
    }
}
