use r2d2;
use std::cell::RefCell;

use crate::cluster::{GetCompressor, GetConnection};
use crate::error;
use crate::frame::{Frame, IntoBytes};
use crate::query::{QueryParams, QueryParamsBuilder, QueryValues, PreparedQuery, PrepareExecutor};
use crate::transport::CDRSTransport;

use super::utils::{prepare_flags, send_frame};
use crate::error::Error;

pub trait ExecExecutor<
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static> + PrepareExecutor<T, M>
{
    fn exec_with_params_tw(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let flags = prepare_flags(with_tracing, with_warnings);
        let options_frame = Frame::new_req_execute(&prepared.id.borrow(), &query_parameters, flags).into_cbytes();

        let mut result = send_frame(self, options_frame);
        if let Err(Error::Server(error)) = &result {
            // if query is unprepared
            if error.error_code == 0x2500 {
                if let Ok(new) = self.prepare_raw(&prepared.query) {
                    prepared.id.replace(new.id);
                    let flags = prepare_flags(with_tracing, with_warnings);
                    let options_frame = Frame::new_req_execute(&prepared.id.borrow(), &query_parameters, flags).into_cbytes();
                    result = send_frame(self, options_frame);
                }
            }
        }
        result
    }

    fn exec_with_params(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.exec_with_params_tw(prepared, query_parameters, false, false)
    }

    fn exec_with_values_tw<V: Into<QueryValues>>(
        &self,
        prepared: &PreparedQuery,
        values: V,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let query_params_builder = QueryParamsBuilder::new();
        let query_params = query_params_builder.values(values.into()).finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
    }

    fn exec_with_values<V: Into<QueryValues>>(
        &self,
        prepared: &PreparedQuery,
        values: V,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.exec_with_values_tw(prepared, values, false, false)
    }

    fn exec_tw(
        &self,
        prepared: &PreparedQuery,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let query_params = QueryParamsBuilder::new().finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
    }

    fn exec(&mut self, prepared: &PreparedQuery) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.exec_tw(prepared, false, false)
    }
}
