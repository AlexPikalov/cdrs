use std::error;
use std::fmt;
use std::io;
use std::result;
use std::string::FromUtf8Error;
use frame::frame_error::CDRSError;
use compression::CompressionError;
use uuid::ParseError;

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
    UUIDParse(ParseError),
    /// General error
    General(String),
    /// Internal error that may be raised during String::from_utf8
    FromUtf8(FromUtf8Error),
    /// Internal Compression/Decompression error
    Compression(CompressionError),
    /// Server error.
    Server(CDRSError)
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
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::Compression(ref err) => err.description(),
            Error::Server(ref err) => err.message.as_str(),
            Error::FromUtf8(ref err) => err.description(),
            // FIXME: err.description not found in current scope, std::error::Error not satisfied
            Error::UUIDParse(_) => "UUID Parse Error",
            Error::General(ref err) => err.as_str()
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

impl From<CompressionError> for Error {
    fn from(err: CompressionError) -> Error {
        return Error::Compression(err);
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        return Error::FromUtf8(err);
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Error {
        return Error::UUIDParse(err);
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        return Error::General(err);
    }
}
