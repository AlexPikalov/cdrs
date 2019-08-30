//!CDRS support traffic decompression as it is described in [Apache
//!Cassandra protocol](
//!https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L790)
//!
//!Before being used, client and server must agree on a compression algorithm to
//!use, which is done in the STARTUP message. As a consequence, a STARTUP message
//!must never be compressed.  However, once the STARTUP frame has been received
//!by the server, messages can be compressed (including the response to the STARTUP
//!request).

use std::error::Error;

/// Compressor trait that defines functionality
/// which should be provided by typical compressor.
pub trait Compressor {
    type CompressorError: Sized + Error;
    /// Encodes given bytes and returns `Result` that contains either
    /// encoded data or an error which occures during the transformation.
    fn encode(&self, bytes: Vec<u8>) -> Result<Vec<u8>, Self::CompressorError>;
    /// Encodes given encoded data and returns `Result` that contains either
    /// encoded bytes or an error which occures during the transformation.
    fn decode(&self, bytes: Vec<u8>) -> Result<Vec<u8>, Self::CompressorError>;
    /// Returns a string which is a name of a compressor. This name should be
    /// exactly the same as one which server returns in a response to
    /// `Options` request.
    fn into_string(&self) -> Option<String>;
}
