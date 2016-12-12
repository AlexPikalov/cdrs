use std::error;
use std::fmt;
use std::io;
use std::result;
use frame::frame_error::CDRSError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Server(CDRSError)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Server(ref err) => write!(f, "Server error: {:?}", err.message),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::Server(ref err) => err.message.as_str(),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        return Error::Io(err);
    }
}
