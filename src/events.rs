use frame::events::{
    ServerEvent as FrameServerEvent,
    SimpleServerEvent as FrameSimpleServerEvent,
    SchemaChange as FrameSchemaChange
};

pub type ServerEvent = FrameServerEvent;
pub type SimpleServerEvent = FrameSimpleServerEvent;
pub type SchemaChange = FrameSchemaChange;
