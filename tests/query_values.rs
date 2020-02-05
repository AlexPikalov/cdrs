#[cfg(feature = "e2e-tests")]
#[macro_use]
extern crate cdrs;

mod common;

#[cfg(feature = "e2e-tests")]
use common::*;

#[cfg(feature = "e2e-tests")]
use cdrs::query::QueryExecutor;
#[cfg(feature = "e2e-tests")]
use cdrs::types::{AsRust, ByName, IntoRustByName};

#[cfg(feature = "e2e-tests")]
use std::str::FromStr;

#[test]
#[cfg(feature = "e2e-tests")]
fn query_values_in() {
  let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_query_values_in \
             (id text PRIMARY KEY)";
  let session = setup(cql).expect("setup");

  session.query(cql).expect("create table error");

  let query_insert = "INSERT INTO cdrs_test.test_query_values_in \
                      (id) VALUES (?)";

  let items = vec!["1".to_string(), "2".to_string(), "3".to_string()];

  for item in items {
    let values = query_values!(item);
    session
      .query_with_values(query_insert, values)
      .expect("insert item error");
  }

  let cql = "SELECT * FROM cdrs_test.test_query_values_in WHERE id IN ?";
  let criteria = vec!["1".to_string(), "3".to_string()];

  let rows = session
    .query_with_values(cql, query_values!(criteria.clone()))
    .expect("select values query error")
    .get_body()
    .expect("get body error")
    .into_rows()
    .expect("converting into rows error");

  assert_eq!(rows.len(), criteria.len());

  let found_all_matching_criteria = criteria.iter().all(|criteria_item: &String| {
    rows.iter().any(|row| {
      let id: String = row.get_r_by_name("id").expect("id");

      criteria_item.clone() == id
    })
  });

  assert!(
    found_all_matching_criteria,
    "should find at least one element for each criteria"
  );
}
