use async_trait::async_trait;
use bb8;
use tokio::sync::Mutex;

use crate::cluster::{GetCompressor, GetConnection};
use crate::error;
use crate::frame::{Frame, IntoBytes};
use crate::query::{QueryParams, QueryParamsBuilder, QueryValues};
use crate::transport::CDRSTransport;
use crate::types::CBytesShort;

use super::utils::{prepare_flags, send_frame};

pub type PreparedQuery = CBytesShort;

#[async_trait]
pub trait ExecExecutor<
    T: CDRSTransport + Unpin + 'static,
    M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static>
{
    async fn exec_with_params_tw(
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
        let options_frame = Frame::new_req_execute(prepared, query_parameters, flags).into_cbytes();

        send_frame(self, options_frame).await
    }

    async fn exec_with_params(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.exec_with_params_tw(prepared, query_parameters, false, false).await
    }

    async fn exec_with_values_tw<V: Into<QueryValues> + Sync + Send>(
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
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings).await
    }

    async fn exec_with_values<V: Into<QueryValues> + Sync + Send>(
        &self,
        prepared: &PreparedQuery,
        values: V,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        self.exec_with_values_tw(prepared, values, false, false).await
    }

    async fn exec_tw(
        &self,
        prepared: &PreparedQuery,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame>
    where
        Self: Sized,
    {
        let query_params = QueryParamsBuilder::new().finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings).await
    }

    async fn exec(&mut self, prepared: &PreparedQuery) -> error::Result<Frame>
    where
        Self: Sized + Sync,
    {
        self.exec_tw(prepared, false, false).await
    }
}
