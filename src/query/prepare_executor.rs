use async_trait::async_trait;
use bb8;
use tokio::sync::Mutex;

use crate::cluster::{GetCompressor, GetConnection, ResponseCache};
use crate::error;
use crate::frame::{Frame, IntoBytes};
use crate::transport::CDRSTransport;
use crate::types::CBytesShort;

use super::utils::{prepare_flags, send_frame};

pub type PreparedQuery = CBytesShort;

#[async_trait]
pub trait PrepareExecutor<
    T: CDRSTransport + Unpin + 'static,
    M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static> + ResponseCache + Sync
{
    /// It prepares a query for execution, along with query itself
    /// the method takes `with_tracing` and `with_warnings` flags
    /// to get tracing information and warnings.
    async fn prepare_tw<Q: ToString + Sync + Send>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<PreparedQuery>
    where
        Self: Sized,
    {
        let flags = prepare_flags(with_tracing, with_warnings);

        let query_frame = Frame::new_req_prepare(query.to_string(), flags);

        send_frame(self, query_frame.into_cbytes(), query_frame.stream)
            .await
            .and_then(|response| response.get_body())
            .and_then(|body| {
                Ok(body
                    .into_prepared()
                    .expect("CDRS BUG: cannot convert frame into prepared")
                    .id)
            })
    }

    /// It prepares query without additional tracing information and warnings.
    async fn prepare<Q: ToString + Sync + Send>(&self, query: Q) -> error::Result<PreparedQuery>
    where
        Self: Sized + Sync,
    {
        self.prepare_tw(query, false, false).await
    }
}
