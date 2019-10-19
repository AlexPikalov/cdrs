use std::cell::RefCell;

use crate::cluster::{GetCompressor, GetConnection};
use crate::error;
use crate::frame::parser::from_connection;
use crate::frame::{Flag, Frame};
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

pub fn send_frame<S, T, M>(sender: &S, frame_bytes: Vec<u8>) -> error::Result<Frame>
where
    S: GetConnection<T, M> + GetCompressor<'static> + Sized,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
{
    let ref compression = sender.get_compressor();

    sender
        .get_connection()
        .ok_or(error::Error::from("Unable to get transport"))
        .and_then(|transport_cell| {
            let write_res = transport_cell
                .borrow_mut()
                .write(frame_bytes.as_slice())
                .map_err(error::Error::from);
            write_res.map(|_| transport_cell)
        })
        .and_then(|transport_cell| from_connection(&transport_cell, compression))
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
