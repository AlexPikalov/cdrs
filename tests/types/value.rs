extern crate byteorder;

use cdrs::IntoBytes;
use cdrs::types::value::{ValueType};

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
