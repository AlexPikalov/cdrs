use consistency::Consistency;
use types::CBytes;
use super::{QueryFlags, QueryParams, QueryValues};

#[derive(Debug, Default)]
pub struct QueryParamsBuilder {
  consistency: Consistency,
  flags: Option<Vec<QueryFlags>>,
  values: Option<QueryValues>,
  with_names: Option<bool>,
  page_size: Option<i32>,
  paging_state: Option<CBytes>,
  serial_consistency: Option<Consistency>,
  timestamp: Option<i64>,
}

impl QueryParamsBuilder {
  /// Factory function that returns new `QueryBuilder`.
  /// Default consistency level is `One`
  pub fn new() -> QueryParamsBuilder {
    Default::default()
  }

  /// Sets new query consistency
  pub fn consistency(mut self, consistency: Consistency) -> Self {
    self.consistency = consistency;

    self
  }

  /// Sets new flags.
  builder_opt_field!(flags, Vec<QueryFlags>);

  /// Sets new values.
  /// Sets new query consistency
  pub fn values(mut self, values: QueryValues) -> Self {
    self.with_names = Some(values.with_names());
    self.values = Some(values);

    self
  }

  /// Sets new with_names parameter value.
  builder_opt_field!(with_names, bool);

  /// Sets new pagesize value.
  builder_opt_field!(page_size, i32);

  /// Sets new paging state value.
  builder_opt_field!(paging_state, CBytes);

  /// Sets new serial_consistency value.
  builder_opt_field!(serial_consistency, Consistency);

  /// Sets new timestamp value.
  builder_opt_field!(timestamp, i64);

  /// Finalizes query building process and returns query itself
  pub fn finalize(self) -> QueryParams {
    QueryParams {
      consistency: self.consistency,
      flags: self.flags.unwrap_or(vec![]),
      values: self.values,
      with_names: self.with_names,
      page_size: self.page_size,
      paging_state: self.paging_state,
      serial_consistency: self.serial_consistency,
      timestamp: self.timestamp,
    }
  }
}
