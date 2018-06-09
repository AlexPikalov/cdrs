use super::QueryParams;

/// Structure that represents CQL query and parameters which will be applied during
/// its execution
#[derive(Debug, Default)]
pub struct Query {
  pub query: String,
  pub params: QueryParams,
}
