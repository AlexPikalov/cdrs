use std::error;
use std::fmt;
use std::io;
use std::result;
use frame::frame_error::CDRSError;

pub type Result<T> = result::Result<T, Error>;

/// CDRS custom error type. CDRS expects two types of error - errors returned by Server
/// and internal erros occured within the driver itself. Ocassionaly `io::Error`
/// is a type that represent internal error because due to implementation IO errors only
/// can be raised by CDRS driver. `Server` error is an error which are ones returned by
/// a Server via result error frames.
#[derive(Debug)]
pub enum Error {
    /// Internal IO error.
    Io(io::Error),
    /// Server error.
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

impl From<CDRSError> for Error {
    fn from(err: CDRSError) -> Error {
        return Error::Server(err);
    }
}
