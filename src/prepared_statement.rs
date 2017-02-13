use compression::Compression;
use consistency::Consistency;
use client::Session;
use authenticators::Authenticator;
use transport::CDRSTransport;
use frame::parser;

use std::collections::HashMap;
use query::{Query, QueryParams, QueryBatch};
use frame::{Frame, Opcode, Flag};
use frame::frame_response::ResponseBody;
use IntoBytes;
use frame::parser::parse_frame;
use types::*;
use frame::events::SimpleServerEvent;

use error;


pub struct PreparedStatement {
    tracing: bool,
    warnings: bool,
    consistency: Consistency,
    with_names: bool,
    page_size: Option<i32>,
    paging_state: Option<i32>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
}

