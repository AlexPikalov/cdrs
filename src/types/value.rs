use super::super::IntoBytes;
use super::*;

/// Types of Cassandra value: normal value (bits), null value and not-set value
#[derive(Debug, Clone)]
pub enum ValueType {
    Normal(i32),
    Null,
    NotSet,
}

impl IntoBytes for ValueType {
    fn into_cbytes(&self) -> Vec<u8> {
        return match *self {
            ValueType::Normal(n) => to_int(n as i64),
            ValueType::Null => i_to_n_bytes(-1, INT_LEN),
            ValueType::NotSet => i_to_n_bytes(-2, INT_LEN),
        };
    }
}

/// Cassandra value which could be an array of bytes, null and non-set values.
#[derive(Debug, Clone)]
pub struct Value {
    pub body: Vec<u8>,
    pub value_type: ValueType,
}

impl Value {
    /// The factory method which creates a normal type value basing on provided bytes.
    pub fn new_normal(body: Vec<u8>) -> Value {
        let l = body.len() as i32;
        return Value {
            body: body,
            value_type: ValueType::Normal(l),
        };
    }

    /// The factory method which creates null Cassandra value.
    pub fn new_null() -> Value {
        return Value {
            body: vec![],
            value_type: ValueType::Null,
        };
    }

    /// The factory method which creates non-set Cassandra value.
    pub fn new_not_set() -> Value {
        return Value {
            body: vec![],
            value_type: ValueType::NotSet,
        };
    }
}

impl IntoBytes for Value {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.value_type.into_cbytes().as_slice());
        v.extend_from_slice(self.body.as_slice());
        return v;
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use IntoBytes;

    #[test]
    fn test_value_type_into_cbytes() {
        // normal value types
        let normal_type = ValueType::Normal(1);
        assert_eq!(normal_type.into_cbytes(), vec![0, 0, 0, 1]);
        // null value types
        let null_type = ValueType::Null;
        assert_eq!(null_type.into_cbytes(), vec![255, 255, 255, 255]);
        // not set value types
        let not_set = ValueType::NotSet;
        assert_eq!(not_set.into_cbytes(), vec![255, 255, 255, 254])
    }

    #[test]
    fn test_new_normal_value() {
        let plain_value = vec![1, 1];
        let len = plain_value.len() as i32;
        let normal_value = Value::new_normal(plain_value.clone());
        assert_eq!(normal_value.body, plain_value);
        match normal_value.value_type {
            ValueType::Normal(l) => assert_eq!(l, len),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_new_null_value() {
        let null_value = Value::new_null();
        assert_eq!(null_value.body, vec![]);
        match null_value.value_type {
            ValueType::Null => assert!(true),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_new_not_set_value() {
        let not_set_value = Value::new_not_set();
        assert_eq!(not_set_value.body, vec![]);
        match not_set_value.value_type {
            ValueType::NotSet => assert!(true),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_value_into_cbytes() {
        let value = Value::new_normal(vec![1]);
        assert_eq!(value.into_cbytes(), vec![0, 0, 0, 1, 1]);
    }

}
