use std::collections::HashMap;
use std::hash::Hash;

use frame::IntoBytes;
use types::value::Value;

/// Enum that represents two types of query values:
/// * values without name
/// * values with names
#[derive(Debug)]
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

  pub fn len(&self) -> usize {
    match *self {
      QueryValues::SimpleValues(ref v) => v.len(),
      QueryValues::NamedValues(ref m) => m.len(),
    }
  }
}

impl<T: Into<Value> + Clone> From<Vec<T>> for QueryValues {
  /// It converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
  fn from(values: Vec<T>) -> QueryValues {
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
      unimplemented!();
    });
    QueryValues::NamedValues(_values)
  }
}

impl IntoBytes for QueryValues {
  fn into_cbytes(&self) -> Vec<u8> {
    unimplemented!()
  }
}
// TODO: implement AsBytes
