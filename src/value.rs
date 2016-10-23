use super::IntoBytes;
use super::types::to_int;

pub enum ValueType {
    Normal(i32),
    Null,
    NotSet
}

impl IntoBytes for ValueType {
    fn into_bytes(&self) -> Vec<u8> {
        return match *self {
            ValueType::Normal(n) => to_int(n as i64),
            ValueType::Null => to_int(-1),
            ValueType::NotSet => to_int(-2)
        };
    }
}

pub struct Value {
    pub body: Vec<u8>,
    pub value_type: ValueType
}

impl Value {
    pub fn new_normal(body: Vec<u8>) -> Value {
        let l = body.len() as i32;
        return Value {
            body: body,
            value_type: ValueType::Normal(l)
        };
    }

    pub fn new_null() -> Value {
        return Value {
            body: vec![],
            value_type: ValueType::Null
        };
    }

    pub fn new_not_set() -> Value {
        return Value {
            body: vec![],
            value_type: ValueType::NotSet
        };
    }
}

impl IntoBytes for Value {
    fn into_bytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.value_type.into_bytes().as_slice());
        v.extend_from_slice(self.body.as_slice());
        return v;
    }
}
