use tokio::sync::Mutex;

use crate::cluster::{GetCompressor, GetConnection, ResponseCache};
use crate::error;
use crate::frame::parser::from_connection;
use crate::frame::{Flag, Frame, StreamId};
use crate::transport::CDRSTransport;

pub fn prepare_flags(with_tracing: bool, with_warnings: bool) -> Vec<Flag> {
    let mut flags = vec![];

    if with_tracing {
        flags.push(Flag::Tracing);
    }

    if with_warnings {
        flags.push(Flag::Warning);
    }

    flags
}

pub async fn send_frame<S, T, M>(sender: &S, frame_bytes: Vec<u8>, stream_id: StreamId) -> error::Result<Frame>
where
    S: GetConnection<T, M> + GetCompressor<'static> + ResponseCache + Sized,
    T: CDRSTransport + Unpin + 'static,
    M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
{
    let ref compression = sender.get_compressor();

    let transport = sender
        .get_connection()
        .await
        .ok_or(error::Error::from("Unable to get transport"))?
        .get_pool();

    let pool = transport
        .get()
        .await
        .map_err(|error| error::Error::from(error.to_string()))?;

    let write_res = pool
        .lock()
        .await
        .write(frame_bytes.as_slice())
        .await
        .map_err(error::Error::from);

    let result = write_res.map(|_| pool);
    match result {
        Ok(ref pool) => loop {
            let frame = from_connection(pool, compression).await?;
            if let Some(frame) = sender.match_or_cache_response(stream_id, frame).await {
                return Ok(frame);
            }
        },
        Err(error) => return Err(error)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn prepare_flags_test() {
        assert_eq!(prepare_flags(true, false), vec![Flag::Tracing]);
        assert_eq!(prepare_flags(false, true), vec![Flag::Warning]);
        assert_eq!(
            prepare_flags(true, true),
            vec![Flag::Tracing, Flag::Warning]
        );
    }
}
