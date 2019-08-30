use std::collections::HashMap;
use std::hash::Hash;

use crate::frame::IntoBytes;
use crate::types::CString;
use crate::types::value::Value;

/// Enum that represents two types of query values:
/// * values without name
/// * values with names
#[derive(Debug, Clone)]
pub enum QueryValues {
  SimpleValues(Vec<Value>),
  NamedValues(HashMap<String, Value>),
}

impl QueryValues {
  /// It returns `true` if query values is with names and `false` otherwise.
  pub fn with_names(&self) -> bool {
    match *self {
      QueryValues::SimpleValues(_) => false,
      _ => true,
    }
  }

  /// It return number of values.
  pub fn len(&self) -> usize {
    match *self {
      QueryValues::SimpleValues(ref v) => v.len(),
      QueryValues::NamedValues(ref m) => m.len(),
    }
  }

  fn named_value_into_bytes_fold(mut bytes: Vec<u8>, vals: (&String, &Value)) -> Vec<u8> {
    let mut name_bytes = CString::new(vals.0.clone()).into_cbytes();
    let mut vals_bytes = vals.1.into_cbytes();
    bytes.append(&mut name_bytes);
    bytes.append(&mut vals_bytes);
    bytes
  }

  fn value_into_bytes_fold(mut bytes: Vec<u8>, val: &Value) -> Vec<u8> {
    let mut val_bytes = val.into_cbytes();
    bytes.append(&mut val_bytes);
    bytes
  }
}

impl<T: Into<Value> + Clone> From<Vec<T>> for QueryValues {
  /// It converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
  fn from(values: Vec<T>) -> QueryValues {
    let vals = values.iter().map(|v| v.clone().into());
    QueryValues::SimpleValues(vals.collect())
  }
}

impl<'a, T: Into<Value> + Clone> From<&'a [T]> for QueryValues {
  /// It converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
  fn from(values: &'a [T]) -> QueryValues {
    let vals = values.iter().map(|v| v.clone().into());
    QueryValues::SimpleValues(vals.collect())
  }
}

impl<S: ToString + Hash + Eq, V: Into<Value> + Clone> From<HashMap<S, V>> for QueryValues {
  /// It converts values from `HashMap` to query values with names `QueryValues::NamedValues`.
  fn from(values: HashMap<S, V>) -> QueryValues {
    let map: HashMap<String, Value> = HashMap::with_capacity(values.len());
    let _values = values.iter().fold(map, |mut acc, v| {
      let name = v.0;
      let val = v.1;
      acc.insert(name.to_string(), val.clone().into());
      acc
    });
    QueryValues::NamedValues(_values)
  }
}

impl IntoBytes for QueryValues {
  fn into_cbytes(&self) -> Vec<u8> {
    let bytes: Vec<u8> = vec![];
    match *self {
      QueryValues::SimpleValues(ref v) => v.iter().fold(bytes, QueryValues::value_into_bytes_fold),
      QueryValues::NamedValues(ref v) => {
        v.iter().fold(bytes, QueryValues::named_value_into_bytes_fold)
      }
    }
  }
}
