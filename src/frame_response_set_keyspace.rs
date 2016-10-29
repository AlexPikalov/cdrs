use super::FromBytes;

pub struct BodyResResultSetKeyspace {
    pub body: String
}

impl BodyResResultSetKeyspace {
    pub fn new(body: String) -> BodyResResultSetKeyspace {
        return BodyResResultSetKeyspace {
            body: body
        }
    }
}

impl FromBytes for BodyResResultSetKeyspace {
    /// Returns BodyResResultSetKeyspace with body provided via bytes
    /// Bytes is Cassandra's [string]
    fn from_bytes(bytes: Vec<u8>) -> BodyResResultSetKeyspace {
        return BodyResResultSetKeyspace::new(String::from_bytes(bytes));
    }
}
