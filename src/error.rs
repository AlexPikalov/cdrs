use std::error;
use std::fmt;
use std::fmt::Display;
use std::io;
use std::result;
use std::string::FromUtf8Error;

use crate::compression::CompressionError;
use crate::frame::frame_error::CDRSError;
use uuid::Error as UUIDError;

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
    /// Internal error that may be raised during `uuid::Uuid::from_bytes`
    UUIDParse(UUIDError),
    /// General error
    General(String),
    /// Internal error that may be raised during `String::from_utf8`
    FromUtf8(FromUtf8Error),
    /// Internal Compression/Decompression error
    Compression(CompressionError),
    /// Server error.
    Server(CDRSError),
}

pub fn column_is_empty_err<T: Display>(column_name: T) -> Error {
    Error::General(format!("Column or UDT property '{}' is empty", column_name))
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Compression(ref err) => write!(f, "Compressor error: {}", err),
            Error::Server(ref err) => write!(f, "Server error: {:?}", err.message),
            Error::FromUtf8(ref err) => write!(f, "FromUtf8Error error: {:?}", err),
            Error::UUIDParse(ref err) => write!(f, "UUIDParse error: {:?}", err),
            Error::General(ref err) => write!(f, "GeneralParsing error: {:?}", err),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::Io(ref e) => Some(e),
            Error::UUIDParse(ref e) => Some(e),
            Error::FromUtf8(ref e) => Some(e),
            Error::Compression(ref e) => Some(e),
            _ => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<CDRSError> for Error {
    fn from(err: CDRSError) -> Error {
        Error::Server(err)
    }
}

impl From<CompressionError> for Error {
    fn from(err: CompressionError) -> Error {
        Error::Compression(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::FromUtf8(err)
    }
}

impl From<UUIDError> for Error {
    fn from(err: UUIDError) -> Error {
        Error::UUIDParse(err)
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::General(err)
    }
}

impl<'a> From<&'a str> for Error {
    fn from(err: &str) -> Error {
        Error::General(err.to_string())
    }
}
