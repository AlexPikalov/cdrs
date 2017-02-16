use super::super::IntoBytes;
use super::*;
use std::convert::Into;

use std::fmt::Debug;

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
            ValueType::Normal(n) => to_int(n),
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
    pub fn new_normal<B>(v: B) -> Value
        where B: Into<Bytes>
    {
        let bytes = v.into().0;
        let l = bytes.len() as i32;
        return Value {
            body: bytes.to_vec(),
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

impl<T: Into<Bytes>> From<T> for Value {
    fn from(b: T) -> Value {
        Value::new_normal(b.into())
    }
}

#[derive(Debug)]
pub struct Bytes(Vec<u8>);

impl Into<Bytes> for String {
    fn into(self) -> Bytes {
        Bytes(self.into_bytes())
    }
}

impl<'a> Into<Bytes> for &'a str {
    fn into(self) -> Bytes {
        Bytes(self.as_bytes().to_vec())
    }
}

impl Into<Bytes> for i8 {
    fn into(self) -> Bytes {
        Bytes(vec![self as u8])
    }
}

impl Into<Bytes> for i16 {
    fn into(self) -> Bytes {
        Bytes(to_short(self))
    }
}

impl Into<Bytes> for i32 {
    fn into(self) -> Bytes {
        Bytes(to_int(self))
    }
}

impl Into<Bytes> for i64 {
    fn into(self) -> Bytes {
        Bytes(to_bigint(self))
    }
}

impl Into<Bytes> for u8 {
    fn into(self) -> Bytes {
        Bytes(vec![self])
    }
}

impl Into<Bytes> for u16 {
    fn into(self) -> Bytes {
        Bytes(to_u_short(self))
    }
}

impl Into<Bytes> for u32 {
    fn into(self) -> Bytes {
        Bytes(to_u(self))
    }
}

impl Into<Bytes> for u64 {
    fn into(self) -> Bytes {
        Bytes(to_u_big(self))
    }
}

impl Into<Bytes> for bool {
    fn into(self) -> Bytes {
        if self { Bytes(vec![1]) } else { Bytes(vec![0]) }
    }
}

impl<T: Into<Bytes> + Clone + Debug> From<Vec<T>> for Bytes {
    fn from(vec: Vec<T>) -> Bytes {
        let mut bytes: Vec<u8> = vec![];
        bytes.extend_from_slice(to_int(vec.len() as i32).as_slice());
        bytes = vec.iter()
            .fold(bytes, |mut acc, v| {
                let b: Bytes = v.clone().into();
                acc.extend_from_slice(Value::new_normal(b).into_cbytes().as_slice());
                acc
            });
        Bytes(bytes)
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
        let plain_value = "hello";
        let len = plain_value.len() as i32;
        let normal_value = Value::new_normal(plain_value);
        assert_eq!(normal_value.body, b"hello");
        match normal_value.value_type {
            ValueType::Normal(l) => assert_eq!(l, len),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_new_normal_value_all_types() {
        let _ = Value::new_normal("hello");
        let _ = Value::new_normal("hello".to_string());
        let _ = Value::new_normal(1 as u8);
        let _ = Value::new_normal(1 as u16);
        let _ = Value::new_normal(1 as u32);
        let _ = Value::new_normal(1 as u64);
        let _ = Value::new_normal(1 as i8);
        let _ = Value::new_normal(1 as i16);
        let _ = Value::new_normal(1 as i32);
        let _ = Value::new_normal(1 as i64);
        let _ = Value::new_normal(true);
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
        let value = Value::new_normal(1 as u8);
        assert_eq!(value.into_cbytes(), vec![0, 0, 0, 1, 1]);
    }

}
