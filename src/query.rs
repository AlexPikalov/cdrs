use types::*;
use types::value::*;
use error::{Result as CResult, Error as CError};
use consistency::Consistency;
use frame::frame_query::{ParamsReqQuery, QueryFlags};
use frame::frame_batch::{BatchType, BatchQuery, BodyReqBatch, BatchQuerySubj};

/// instead of writing functions which resemble
/// ```
/// pub fn query<'a> (&'a mut self,query: String) -> &'a mut Self{
///     self.query = Some(query);
///            self
/// }
/// ```
/// and repeating it for all the attributes; it is extracted out as a macro so that code
/// is more concise see
/// @https://doc.rust-lang.org/book/method-syntax.html
///
///
///
macro_rules! builder_opt_field {
    ($field:ident, $field_type:ty) => {
        pub fn $field(mut self,
                          $field: $field_type) -> Self {
            self.$field = Some($field);
            self
        }
    };
}

/// Structure that represents CQL query and parameters which will be applied during
/// its execution
#[derive(Debug, Default)]
pub struct Query {
    pub query: String,
    // query parameters
    pub consistency: Consistency,
    pub values: Option<Vec<Value>>,
    pub with_names: Option<bool>,
    pub page_size: Option<i32>,
    pub paging_state: Option<CBytes>,
    pub serial_consistency: Option<Consistency>,
    pub timestamp: Option<i64>,
}

/// QueryBuilder is a helper sturcture that helps to construct `Query`. `Query` itself
/// consists of CQL query string and list of parameters.
/// Parameters are the same as ones described in [Cassandra v4 protocol]
/// (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L304)
#[derive(Debug, Default)]
pub struct QueryBuilder {
    query: String,
    consistency: Consistency,
    values: Option<Vec<Value>>,
    with_names: Option<bool>,
    page_size: Option<i32>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
}

impl QueryBuilder {
    /// Factory function that takes CQL as an argument and returns new `QueryBuilder`.
    /// Default consistency level is `One`
    pub fn new<T: Into<String>>(query: T) -> QueryBuilder {
        return QueryBuilder { query: query.into(), ..Default::default() };
    }

    /// Sets new query consistency
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;

        return self;
    }

    /// Sets new query values
    builder_opt_field!(values, Vec<Value>);

    /// Sets new query with_names
    builder_opt_field!(with_names, bool);

    /// Sets new query pagesize
    builder_opt_field!(page_size, i32);

    /// Sets new query pagin state
    builder_opt_field!(paging_state, CBytes);

    /// Sets new query serial_consistency
    builder_opt_field!(serial_consistency, Consistency);

    /// Sets new quey timestamp
    builder_opt_field!(timestamp, i64);

    pub fn apply_query_params(mut self, params: QueryParams) -> Self {
        self.consistency = params.consistency;
        self.values = params.values;
        self.page_size = params.page_size;
        self.paging_state = params.paging_state;
        self.serial_consistency = params.serial_consistency;
        self.timestamp = params.timestamp;

        return self;
    }

    /// Finalizes query building process and returns query itself
    pub fn finalize(self) -> Query {
        return Query {
            query: self.query,
            consistency: self.consistency,
            values: self.values,
            with_names: self.with_names,
            page_size: self.page_size,
            paging_state: self.paging_state,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
        };
    }
}

pub type QueryParams = ParamsReqQuery;

/// Query parameters builder
#[derive(Debug)]
pub struct QueryParamsBuilder {
    consistency: Consistency,
    values: Option<Vec<Value>>,
    with_names: bool,
    page_size: Option<i32>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
}

impl QueryParamsBuilder {
    pub fn new(consistency: Consistency) -> QueryParamsBuilder {
        return QueryParamsBuilder {
            consistency: consistency,
            values: None,
            with_names: false,
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            timestamp: None,
        };
    }

    pub fn values(mut self, v: Vec<Value>) -> Self {
        self.values = Some(v);

        return self;
    }

    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;

        return self;
    }

    pub fn page_size(mut self, page_size: i32) -> Self {
        self.page_size = Some(page_size);

        return self;
    }

    pub fn paging_state(mut self, paging_state: CBytes) -> Self {
        self.paging_state = Some(paging_state);

        return self;
    }

    pub fn serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency = Some(serial_consistency);

        return self;
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);

        return self;
    }

    pub fn finalize(self) -> QueryParams {
        // query flags
        let mut flags: Vec<QueryFlags> = vec![];

        if self.values.is_some() {
            flags.push(QueryFlags::Value);
        }

        if self.with_names {
            flags.push(QueryFlags::WithNamesForValues);
        }

        if self.page_size.is_some() {
            flags.push(QueryFlags::PageSize);
        }

        if self.serial_consistency.is_some() {
            flags.push(QueryFlags::WithSerialConsistency);
        }

        if self.timestamp.is_some() {
            flags.push(QueryFlags::WithDefaultTimestamp);
        }

        QueryParams {
            consistency: self.consistency,
            flags: flags,
            values: self.values,
            page_size: self.page_size,
            paging_state: self.paging_state,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
        }

    }
}

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
    pub fn add_query<T: Into<String>>(mut self, query: T, values: Vec<BatchValue>) -> Self {
        self.queries.push(BatchQuery {
            is_prepared: false,
            subject: BatchQuerySubj::QueryString(CStringLong::new(query.into())),
            values: values,
        });
        self
    }

    /// Add a query (prepared one)
    pub fn add_query_prepared(mut self, query_id: CBytesShort, values: Vec<BatchValue>) -> Self {
        self.queries.push(BatchQuery {
            is_prepared: true,
            subject: BatchQuerySubj::PreparedId(query_id),
            values: values,
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

        let with_names_for_values = self.queries
            .iter()
            .all(|q| q.values.iter().all(|v| v.0.is_some()));

        if !with_names_for_values {
            let some_names_for_values = self.queries
                .iter()
                .any(|q| q.values.iter().any(|v| v.0.is_some()));

            if some_names_for_values {
                return Err(CError::General(String::from("Inconsistent query values - mixed \
                                                         with and without names values")));
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

pub type BatchValue = (Option<CString>, Value);

#[cfg(test)]
mod query_builder {
    use super::*;

    #[test]
    fn new() {
        let _ = QueryBuilder::new("USE keyspace").finalize();
    }

    #[test]
    fn with_parameters() {
        let _ = QueryBuilder::new("USE keyspace")
            .consistency(Consistency::Two)
            .values(vec![Value::new_null()])
            .with_names(true)
            .page_size(11)
            .paging_state(CBytes::new(vec![1, 2, 3, 4, 5]))
            .serial_consistency(Consistency::One)
            .timestamp(1245678)
            .finalize();
    }
}

#[cfg(test)]
mod query_params_builder {
    use super::*;

    #[test]
    fn new() {
        let _ = QueryParamsBuilder::new(Consistency::Two).finalize();
    }

    #[test]
    fn with_parameters() {
        let _ = QueryParamsBuilder::new(Consistency::Two)
            .values(vec![Value::new_null()])
            .with_names(true)
            .page_size(11)
            .paging_state(CBytes::new(vec![1, 2, 3, 4, 5]))
            .serial_consistency(Consistency::One)
            .timestamp(1245678)
            .finalize();
    }
}

#[cfg(test)]
mod batch_query_builder {
    use super::*;

    #[test]
    fn new() {
        assert!(BatchQueryBuilder::new().finalize().is_ok());
    }

    #[test]
    fn with_parameters() {
        let q = BatchQueryBuilder::new()
            .batch_type(BatchType::Logged)
            .add_query("some query".to_string(), vec![])
            .add_query_prepared(CBytesShort::new(vec![1, 2, 3]), vec![])
            .consistency(Consistency::One)
            .serial_consistency(Some(Consistency::Two))
            .timestamp(Some(1234))
            .finalize();
        assert!(q.is_ok())
    }

    #[test]
    fn clear_queries() {
        let q = BatchQueryBuilder::new()
            .add_query("some query".to_string(), vec![])
            .clear_queries()
            .finalize();
        assert!(q.is_ok())
    }
}
