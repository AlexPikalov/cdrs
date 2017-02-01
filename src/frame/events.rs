use frame::frame_result::BodyResResultSchemaChange;

const TOPOLOGY_CHANGE: &'static str = "TOPOLOGY_CHANGE";
const STATUS_CHANGE: &'static str = "STATUS_CHANGE";
const SCHEMA_CHANGE: &'static str = "SCHEMA_CHANGE";

#[derive(Debug)]
pub enum ServerEvent {
    TopologyChange,
    StatusChange,
    SchemaChange(BodyResResultSchemaChange)
}

impl ServerEvent {
    pub fn as_string(&self) -> String {
        match self {
            &ServerEvent::TopologyChange => String::from(TOPOLOGY_CHANGE),
            &ServerEvent::StatusChange => String::from(STATUS_CHANGE),
            &ServerEvent::SchemaChange(_) => String::from(SCHEMA_CHANGE)
        }
    }
}
