use super::FromBytes;

pub struct BodyResSetKeyspace {
    pub body: String
}

impl BodyResSetKeyspace {
    pub fn new(body: String) -> BodyResSetKeyspace {
        return BodyResSetKeyspace {
            body: body
        }
    }
}

impl FromBytes for BodyResSetKeyspace {
    /// Returns BodyResSetKeyspace with body provided via bytes
    /// Bytes is Cassandra's [string]
    fn from_bytes(bytes: Vec<u8>) -> BodyResSetKeyspace {
        return BodyResSetKeyspace::new(String::from_bytes(bytes));
    }
}
