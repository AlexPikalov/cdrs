extern crate byteorder;

use cdrs::IntoBytes;
use cdrs::types::value::{ValueType, Value};

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
        _ => unreachable!()
    }
}

#[test]
fn test_new_null_value() {
    let null_value = Value::new_null();
    assert_eq!(null_value.body, vec![]);
    match null_value.value_type {
        ValueType::Null => assert!(true),
        _ => unreachable!()
    }
}

#[test]
fn test_new_not_set_value() {
    let not_set_value = Value::new_not_set();
    assert_eq!(not_set_value.body, vec![]);
    match not_set_value.value_type {
        ValueType::NotSet => assert!(true),
        _ => unreachable!()
    }
}

#[test]
fn test_value_into_cbytes() {
    let value = Value::new_normal(vec![1]);
    assert_eq!(value.into_cbytes(), vec![0, 0, 0, 1, 1]);
}
