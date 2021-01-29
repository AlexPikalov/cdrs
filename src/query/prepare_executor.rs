use r2d2;
use std::cell::RefCell;

use crate::cluster::{GetCompressor, GetConnection};
use crate::error;
use crate::frame::frame_result::BodyResResultPrepared;
use crate::frame::{Frame, IntoBytes};
use crate::transport::CDRSTransport;

use super::utils::{prepare_flags, send_frame};
use crate::query::PreparedQuery;

pub trait PrepareExecutor<
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static>
{
    /// It prepares a query for execution, along with query itself the
    /// method takes `with_tracing` and `with_warnings` flags to get
    /// tracing information and warnings. Return the raw prepared
    /// query result.
    fn prepare_raw_tw<Q: ToString>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<BodyResResultPrepared>
    where
        Self: Sized,
    {
        let flags = prepare_flags(with_tracing, with_warnings);

        let query_frame = Frame::new_req_prepare(query.to_string(), flags).into_cbytes();

        send_frame(self, query_frame)
            .and_then(|response| response.get_body())
            .and_then(|body| {
                Ok(body
                    .into_prepared()
                    .expect("CDRS BUG: cannot convert frame into prepared"))
            })
    }

    /// It prepares query without additional tracing information and warnings.
    /// Return the raw prepared query result.
    fn prepare_raw<Q: ToString>(&self, query: Q) -> error::Result<BodyResResultPrepared>
    where
        Self: Sized,
    {
        self.prepare_raw_tw(query, false, false)
    }

    /// It prepares a query for execution, along with query itself
    /// the method takes `with_tracing` and `with_warnings` flags
    /// to get tracing information and warnings. Return the prepared
    /// query ID.
    fn prepare_tw<Q: ToString>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<PreparedQuery>
    where
        Self: Sized,
    {
        let str = query.to_string();
        self.prepare_raw_tw(query, with_tracing, with_warnings)
            .map(|x| PreparedQuery { id: RefCell::new(x.id), query: str })
    }

    /// It prepares query without additional tracing information and warnings.
    /// Return the prepared query ID.
    fn prepare<Q: ToString>(&self, query: Q) -> error::Result<PreparedQuery>
    where
        Self: Sized,
    {
        self.prepare_tw(query, false, false)
    }
}
