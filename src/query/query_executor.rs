use r2d2;
use std::cell::RefCell;

use crate::cluster::{GetCompressor, GetConnection};
use crate::error;
use crate::frame::{Frame, IntoBytes};
use crate::query::{Query, QueryParams, QueryParamsBuilder, QueryValues};
use crate::transport::CDRSTransport;

use super::utils::{prepare_flags, send_frame};

pub trait QueryExecutor<
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static>
{
    fn query_with_params_tw<Q: ToString>(
        &self,
        query: Q,
        query_params: QueryParams,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let query = Query {
            query: query.to_string(),
            params: query_params,
        };

        let flags = prepare_flags(with_tracing, with_warnings);

        let query_frame = Frame::new_query(query, flags).into_cbytes();

        send_frame(self, query_frame)
    }

    /// Executes a query with default parameters:
    /// * TDB
    fn query<Q: ToString>(&self, query: Q) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.query_tw(query, false, false)
    }

    /// Executes a query with ability to trace it and see warnings, and default parameters:
    /// * TBD
    fn query_tw<Q: ToString>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let query_params = QueryParamsBuilder::new().finalize();
        self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
    }

    /// Executes a query with bounded values (either with or without names).
    fn query_with_values<Q: ToString, V: Into<QueryValues>>(
        &self,
        query: Q,
        values: V,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.query_with_values_tw(query, values, false, false)
    }

    /// Executes a query with bounded values (either with or without names)
    /// and ability to see warnings, trace a request and default parameters.
    fn query_with_values_tw<Q: ToString, V: Into<QueryValues>>(
        &self,
        query: Q,
        values: V,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let query_params_builder = QueryParamsBuilder::new();
        let query_params = query_params_builder.values(values.into()).finalize();
        self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
    }

    /// Executes a query with query params without warnings and tracing.
    fn query_with_params<Q: ToString>(
        &self,
        query: Q,
        query_params: QueryParams,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.query_with_params_tw(query, query_params, false, false)
    }
}
