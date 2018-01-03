use error;
use frame::frame_result::{RowsMetadata, RowsMetadataFlag};
use cluster::session::Session;
use query::{ExecExecutor, PreparedQuery, QueryExecutor, QueryParamsBuilder};
use types::rows::Row;
use types::CBytes;
use transport::TransportTcp;
use load_balancing::LoadBalancingStrategy;

pub struct SessionPager<'a, LB: 'a> {
  page_size: i32,
  session: &'a mut Session<LB>,
}

impl<'a, LB: 'a> SessionPager<'a, LB> {
  pub fn new(session: &'a mut Session<LB>, page_size: i32) -> SessionPager<LB> {
    SessionPager { session, page_size }
  }

  pub fn query<Q>(&'a mut self, query: Q) -> QueryPager<'a, LB, Q>
    where Q: ToString,
          LB: 'a
  {
    QueryPager { pager: self,
                 paging_state: None,
                 has_more_pages: None,
                 query, }
  }

  pub fn exec(&'a mut self, query: &'a PreparedQuery) -> ExecPager<'a, LB>
    where LB: 'a
  {
    ExecPager { pager: self,
                paging_state: None,
                has_more_pages: None,
                query, }
  }
}

pub struct QueryPager<'a, LB: 'a, Q: ToString> {
  pager: &'a mut SessionPager<'a, LB>,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: Q,
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized, Q: ToString> QueryPager<'a, LB, Q> {
  pub fn next(&'a mut self) -> error::Result<Vec<Row>> {
    let mut params = QueryParamsBuilder::new().page_size(self.pager.page_size);
    if self.paging_state.is_some() {
      params = params.paging_state(self.paging_state.clone().unwrap());
    }

    let body = self.pager
                   .session
                   .query_with_params(self.query.to_string(), params.finalize())
                   .and_then(|frame| frame.get_body())?;

    let metadata_res: error::Result<RowsMetadata> =
      body.as_rows_metadata()
          .ok_or("Pager query should yield a vector of rows".into());
    let metadata = metadata_res?;

    self.has_more_pages = Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()));
    body.into_rows()
        .ok_or("Pager query should yield a vector of rows".into())
  }

  pub fn is_last(&self) -> bool {
    !self.has_more_pages.unwrap_or(false)
  }
}

pub struct ExecPager<'a, LB: 'a> {
  pager: &'a mut SessionPager<'a, LB>,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: &'a PreparedQuery,
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> ExecPager<'a, LB> {
  pub fn next(&'a mut self) -> error::Result<Vec<Row>> {
    let mut params = QueryParamsBuilder::new().page_size(self.pager.page_size);
    if self.paging_state.is_some() {
      params = params.paging_state(self.paging_state.clone().unwrap());
    }

    let body = self.pager
                   .session
                   .exec_with_params(self.query, params.finalize())
                   .and_then(|frame| frame.get_body())?;

    let metadata_res: error::Result<RowsMetadata> =
      body.as_rows_metadata()
          .ok_or("Pager query should yield a vector of rows".into());
    let metadata = metadata_res?;

    self.has_more_pages = Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()));
    body.into_rows()
        .ok_or("Pager query should yield a vector of rows".into())
  }

  pub fn is_last(&self) -> bool {
    !self.has_more_pages.unwrap_or(false)
  }
}
