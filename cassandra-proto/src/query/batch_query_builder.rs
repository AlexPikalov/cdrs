use super::{QueryFlags, QueryValues};
use crate::consistency::Consistency;
use crate::error::{Error as CError, Result as CResult};
use crate::frame::frame_batch::{BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch};
use crate::types::{CBytesShort, CStringLong};

pub type QueryBatch = BodyReqBatch;

#[derive(Debug)]
pub struct BatchQueryBuilder {
  batch_type: BatchType,
  queries: Vec<BatchQuery>,
  consistency: Consistency,
  serial_consistency: Option<Consistency>,
  timestamp: Option<i64>,
}

impl BatchQueryBuilder {
  pub fn new() -> BatchQueryBuilder {
    BatchQueryBuilder {
      batch_type: BatchType::Logged,
      queries: vec![],
      consistency: Consistency::One,
      serial_consistency: None,
      timestamp: None,
    }
  }

  pub fn batch_type(mut self, batch_type: BatchType) -> Self {
    self.batch_type = batch_type;
    self
  }

  /// Add a query (non-prepared one)
  pub fn add_query<T: Into<String>>(mut self, query: T, values: QueryValues) -> Self {
    self.queries.push(BatchQuery {
      is_prepared: false,
      subject: BatchQuerySubj::QueryString(CStringLong::new(query.into())),
      values,
    });
    self
  }

  /// Add a query (prepared one)
  pub fn add_query_prepared(mut self, query_id: CBytesShort, values: QueryValues) -> Self {
    self.queries.push(BatchQuery {
      is_prepared: true,
      subject: BatchQuerySubj::PreparedId(query_id),
      values,
    });
    self
  }

  pub fn clear_queries(mut self) -> Self {
    self.queries = vec![];
    self
  }

  pub fn consistency(mut self, consistency: Consistency) -> Self {
    self.consistency = consistency;
    self
  }

  pub fn serial_consistency(mut self, serial_consistency: Option<Consistency>) -> Self {
    self.serial_consistency = serial_consistency;
    self
  }

  pub fn timestamp(mut self, timestamp: Option<i64>) -> Self {
    self.timestamp = timestamp;
    self
  }

  pub fn finalize(self) -> CResult<BodyReqBatch> {
    let mut flags = vec![];

    if self.serial_consistency.is_some() {
      flags.push(QueryFlags::WithSerialConsistency);
    }

    if self.timestamp.is_some() {
      flags.push(QueryFlags::WithDefaultTimestamp);
    }

    let with_names_for_values = self.queries.iter().all(|q| q.values.with_names());

    if !with_names_for_values {
      let some_names_for_values = self.queries.iter().any(|q| q.values.with_names());

      if some_names_for_values {
        return Err(CError::General(String::from(
          "Inconsistent query values - mixed \
           with and without names values",
        )));
      }
    }

    if with_names_for_values {
      flags.push(QueryFlags::WithNamesForValues);
    }

    Ok(BodyReqBatch {
      batch_type: self.batch_type,
      queries: self.queries,
      query_flags: flags,
      consistency: self.consistency,
      serial_consistency: self.serial_consistency,
      timestamp: self.timestamp,
    })
  }
}
