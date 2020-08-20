use std::cell::RefCell;
use crate::types::CBytesShort;

#[derive(Debug, Clone)]
pub struct PreparedQuery {
	pub(crate) id: RefCell<CBytesShort>,
	pub(crate) query: String,
}
