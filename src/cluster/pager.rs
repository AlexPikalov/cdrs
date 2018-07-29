use std::marker::PhantomData;

use cluster::CDRSSession;
use error;
use frame::frame_result::{RowsMetadata, RowsMetadataFlag};
use query::{PreparedQuery, QueryParamsBuilder};
use transport::CDRSTransport;
use types::rows::Row;
use types::CBytes;

pub struct SessionPager<'a, S: CDRSSession<'static, T> + 'a, T: CDRSTransport + 'static> {
  page_size: i32,
  session: &'a S,
  transport_type: PhantomData<&'a T>,
}

impl<'a, 'b: 'a, S: CDRSSession<'static, T>, T: CDRSTransport + 'static> SessionPager<'a, S, T> {
  pub fn new(session: &'b S, page_size: i32) -> SessionPager<'a, S, T> {
    SessionPager {
      session,
      page_size,
      transport_type: PhantomData,
    }
  }

  pub fn query<Q>(&'a mut self, query: Q) -> QueryPager<'a, Q, SessionPager<'a, S, T>>
  where
    Q: ToString,
  {
    QueryPager {
      pager: self,
      paging_state: None,
      has_more_pages: None,
      query,
    }
  }

  pub fn exec(&'a mut self, query: &'a PreparedQuery) -> ExecPager<'a, SessionPager<'a, S, T>> {
    ExecPager {
      pager: self,
      paging_state: None,
      has_more_pages: None,
      query,
    }
  }
}

pub struct QueryPager<'a, Q: ToString, P: 'a> {
  pager: &'a mut P,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: Q,
}

impl<'a, Q: ToString, T: CDRSTransport + 'static, S: CDRSSession<'static, T>>
  QueryPager<'a, Q, SessionPager<'a, S, T>>
{
  pub fn next(&mut self) -> error::Result<Vec<Row>> {
    let mut params = QueryParamsBuilder::new().page_size(self.pager.page_size);
    if self.paging_state.is_some() {
      params = params.paging_state(self.paging_state.clone().unwrap());
    }

    let body = self
      .pager
      .session
      .query_with_params(self.query.to_string(), params.finalize())
      .and_then(|frame| frame.get_body())?;

    let metadata_res: error::Result<RowsMetadata> = body
      .as_rows_metadata()
      .ok_or("Pager query should yield a vector of rows".into());
    let metadata = metadata_res?;

    self.has_more_pages = Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()));
    self.paging_state = metadata.paging_state.clone();
    body
      .into_rows()
      .ok_or("Pager query should yield a vector of rows".into())
  }

  pub fn has_more(&self) -> bool {
    self.has_more_pages.unwrap_or(false)
  }
}

pub struct ExecPager<'a, P: 'a> {
  pager: &'a mut P,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: &'a PreparedQuery,
}

impl<'a, T: CDRSTransport + 'static, S: CDRSSession<'static, T>>
  ExecPager<'a, SessionPager<'a, S, T>>
{
  pub fn next(&mut self) -> error::Result<Vec<Row>> {
    let mut params = QueryParamsBuilder::new().page_size(self.pager.page_size);
    if self.paging_state.is_some() {
      params = params.paging_state(self.paging_state.clone().unwrap());
    }

    let body = self
      .pager
      .session
      .exec_with_params(self.query, params.finalize())
      .and_then(|frame| frame.get_body())?;

    let metadata_res: error::Result<RowsMetadata> = body
      .as_rows_metadata()
      .ok_or("Pager query should yield a vector of rows".into());
    let metadata = metadata_res?;

    self.has_more_pages = Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()));
    self.paging_state = metadata.paging_state.clone();
    body
      .into_rows()
      .ok_or("Pager query should yield a vector of rows".into())
  }

  pub fn has_more(&self) -> bool {
    self.has_more_pages.unwrap_or(false)
  }
}
