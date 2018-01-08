use consistency::Consistency;
use types::{to_bigint, to_int, to_short, CBytes};
use frame::AsByte;
use frame::IntoBytes;
use query::query_flags::QueryFlags;
use query::query_values::QueryValues;

/// Parameters of Query for query operation.
#[derive(Debug, Default)]
pub struct QueryParams {
  /// Cassandra consistency level.
  pub consistency: Consistency,
  /// Array of query flags.
  pub flags: Vec<QueryFlags>,
  /// Were values provided with names
  pub with_names: Option<bool>,
  /// Array of values.
  pub values: Option<QueryValues>,
  /// Page size.
  pub page_size: Option<i32>,
  /// Array of bytes which represents paging state.
  pub paging_state: Option<CBytes>,
  /// Serial `Consistency`.
  pub serial_consistency: Option<Consistency>,
  /// Timestamp.
  pub timestamp: Option<i64>,
}

impl QueryParams {
  /// Sets values of Query request params.
  pub fn set_values(&mut self, values: QueryValues) {
    self.flags.push(QueryFlags::Value);
    self.values = Some(values);
  }

  fn flags_as_byte(&self) -> u8 {
    self.flags.iter().fold(0, |acc, flag| acc | flag.as_byte())
  }

  #[allow(dead_code)]
  fn parse_query_flags(byte: u8) -> Vec<QueryFlags> {
    let mut flags: Vec<QueryFlags> = vec![];

    if QueryFlags::has_value(byte) {
      flags.push(QueryFlags::Value);
    }
    if QueryFlags::has_skip_metadata(byte) {
      flags.push(QueryFlags::SkipMetadata);
    }
    if QueryFlags::has_page_size(byte) {
      flags.push(QueryFlags::PageSize);
    }
    if QueryFlags::has_with_paging_state(byte) {
      flags.push(QueryFlags::WithPagingState);
    }
    if QueryFlags::has_with_serial_consistency(byte) {
      flags.push(QueryFlags::WithSerialConsistency);
    }
    if QueryFlags::has_with_default_timestamp(byte) {
      flags.push(QueryFlags::WithDefaultTimestamp);
    }
    if QueryFlags::has_with_names_for_values(byte) {
      flags.push(QueryFlags::WithNamesForValues);
    }

    flags
  }
}

impl IntoBytes for QueryParams {
  fn into_cbytes(&self) -> Vec<u8> {
    let mut v: Vec<u8> = vec![];

    v.extend_from_slice(self.consistency.into_cbytes().as_slice());
    v.push(self.flags_as_byte());
    if QueryFlags::has_value(self.flags_as_byte()) {
      if let Some(ref values) = self.values {
        v.extend_from_slice(to_short(values.len() as i16).as_slice());
        v.extend_from_slice(values.into_cbytes().as_slice());
      }
    }
    if QueryFlags::has_with_paging_state(self.flags_as_byte()) && self.paging_state.is_some() {
      // XXX clone
      v.extend_from_slice(self.paging_state
                                    .clone()
                                    // unwrap is safe as we've checked that
                                    // self.paging_state.is_some()
                                    .unwrap()
                                    .into_cbytes()
                                    .as_slice());
    }
    if QueryFlags::has_page_size(self.flags_as_byte()) && self.page_size.is_some() {
      // XXX clone
      v.extend_from_slice(to_int(self.page_size
                                    .clone()
                                    // unwrap is safe as we've checked that
                                    // self.page_size.is_some()
                                    .unwrap())
                                    .as_slice());
    }
    if QueryFlags::has_with_serial_consistency(self.flags_as_byte())
       && self.serial_consistency.is_some() {
      // XXX clone
      v.extend_from_slice(self.serial_consistency
                                    .clone()
                                    // unwrap is safe as we've checked that
                                    // self.serial_consistency.is_some()
                                    .unwrap()
                                    .into_cbytes()
                                    .as_slice());
    }
    if QueryFlags::has_with_default_timestamp(self.flags_as_byte()) && self.timestamp.is_some() {
      // unwrap is safe as we've checked that self.timestamp.is_some()
      v.extend_from_slice(to_bigint(self.timestamp.unwrap()).as_slice());
    }

    v
  }
}
