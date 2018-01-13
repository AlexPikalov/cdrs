use std::cell::RefCell;
use std::rc::Rc;

use error;
use authenticators::Authenticator;
use frame::frame_result::{RowsMetadata, RowsMetadataFlag};
use cluster::session::Session;
use query::{ExecExecutor, PreparedQuery, QueryExecutor, QueryParamsBuilder};
use types::rows::Row;
use types::CBytes;
use transport::TransportTcp;
use load_balancing::LoadBalancingStrategy;

pub struct SessionPager<'a, LB: 'a, A: 'a> {
  page_size: i32,
  session: &'a mut Session<LB, A>,
}

impl<'a, 'b: 'a, LB: 'a, A: 'a> SessionPager<'a, LB, A> {
  pub fn new(session: &'b mut Session<LB, A>, page_size: i32) -> SessionPager<'a, LB, A> {
    SessionPager { session, page_size }
  }

  pub fn query<Q>(&'a mut self, query: Q) -> QueryPager<'a, LB, A, Q>
    where Q: ToString
  {
    QueryPager { pager: self,
                 paging_state: None,
                 has_more_pages: None,
                 query, }
  }

  pub fn exec(&'a mut self, query: &'a PreparedQuery) -> ExecPager<'a, LB, A> {
    ExecPager { pager: self,
                paging_state: None,
                has_more_pages: None,
                query, }
  }
}

pub struct QueryPager<'a, LB: 'a, A: 'a, Q: ToString> {
  pager: &'a mut SessionPager<'a, LB, A>,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: Q,
}

impl<'a,
     LB: LoadBalancingStrategy<TransportTcp> + Sized,
     Q: ToString,
     A: Authenticator + Sized> QueryPager<'a, LB, A, Q> {
  pub fn next(&mut self) -> error::Result<Vec<Row>> {
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
    self.paging_state = metadata.paging_state.clone();
    body.into_rows()
        .ok_or("Pager query should yield a vector of rows".into())
  }

  pub fn has_more(&self) -> bool {
    self.has_more_pages.unwrap_or(false)
  }
}

pub struct ExecPager<'a, LB: 'a, A: 'a> {
  pager: &'a mut SessionPager<'a, LB, A>,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: &'a PreparedQuery,
}

impl<'a, LB: LoadBalancingStrategy<TransportTcp> + Sized, A: Authenticator + Sized>
  ExecPager<'a, LB, A> {
  pub fn next(&mut self) -> error::Result<Vec<Row>> {
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
    self.paging_state = metadata.paging_state.clone();
    body.into_rows()
        .ok_or("Pager query should yield a vector of rows".into())
  }

  pub fn has_more(&self) -> bool {
    self.has_more_pages.unwrap_or(false)
  }
}
