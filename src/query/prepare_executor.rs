use error;
use frame::Frame;
use types::CBytesShort;

pub type PreparedQuery = CBytesShort;

pub trait PrepareExecutor<'a> {
  /// It prepares a query for execution, along with query itself
  /// the method takes `with_tracing` and `with_warnings` flags
  /// to get tracing information and warnings.
  fn prepare_tw<Q: ToString>(&'a mut self,
                             query: Q,
                             with_tracing: bool,
                             with_warnings: bool)
                             -> error::Result<PreparedQuery>;

  /// It prepares query without additional tracing information and warnings.
  fn prepare<Q: ToString>(&'a mut self, query: Q) -> error::Result<PreparedQuery> {
    self.prepare_tw(query, false, false)
  }
}
