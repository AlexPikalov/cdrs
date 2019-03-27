use r2d2;
use std::cell::RefCell;
use std::marker::PhantomData;

use cluster::CDRSSession;
use error;
use frame::frame_result::{RowsMetadata, RowsMetadataFlag};
use query::{PreparedQuery, QueryParamsBuilder};
use transport::CDRSTransport;
use types::rows::Row;
use types::CBytes;

pub struct SessionPager<
  'a,
  M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  S: CDRSSession<'static, T, M> + 'a,
  T: CDRSTransport + 'static,
> {
  page_size: i32,
  session: &'a S,
  transport_type: PhantomData<&'a T>,
  connection_type: PhantomData<&'a M>,
}

impl<
    'a,
    'b: 'a,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    S: CDRSSession<'static, T, M>,
    T: CDRSTransport + 'static,
  > SessionPager<'a, M, S, T>
{
  pub fn new(session: &'b S, page_size: i32) -> SessionPager<'a, M, S, T> {
    SessionPager {
      session,
      page_size,
      transport_type: PhantomData,
      connection_type: PhantomData,
    }
  }


  /// Java native driver also uses this technique to
  /// perform stateless pagination
  /// (https://docs.datastax.com/en/developer/java-driver/3.2/manual/paging/)
  pub fn query_with_pager_state<Q>(&'a mut self, query: Q, state: Option<String>) -> Result<QueryPager<'a, Q, SessionPager<'a, M, S, T>>, error::Error>
    where
      Q: ToString,
  {
    let tmp_state: Option<Result<Option<CBytes>, &str>> = state
      .map(|st| {
        if st.is_empty() || st.len() % 3 != 0 {
          return Ok(None);
        }

        let cap: usize = st.len() / 3;

        let mut vec: Vec<u8> = Vec::with_capacity(cap);

        let mut p = st.chars().peekable();

        while p.peek().is_some() {
          let chunk: String = p.by_ref().take(3).collect();
          vec.push(chunk.parse::<u8>().map_err(|_p_err| "String must be composed by digits")?);
        }

        Ok(Some(CBytes::new(vec)))
      });

    match tmp_state {
      Some(r_state) => match r_state {
        Ok(s_state) => {
          Ok(QueryPager {
            pager: self,
            paging_state: s_state,
            has_more_pages: None,
            query,
          })
        }
        Err(err) => Err(error::Error::General(err.to_string()))
      },
      None => Ok(self.query(query)),
    }
  }

  pub fn query<Q>(&'a mut self, query: Q) -> QueryPager<'a, Q, SessionPager<'a, M, S, T>>
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

  pub fn exec(&'a mut self, query: &'a PreparedQuery) -> ExecPager<'a, SessionPager<'a, M, S, T>> {
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

impl<
    'a,
    Q: ToString,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    S: CDRSSession<'static, T, M>,
  > QueryPager<'a, Q, SessionPager<'a, M, S, T>>
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


  /// Unsigned byte is characterized by one byte or 8 bits.
  /// Each unsigned byte can represent a number from 0 to 255,
  /// so we can safely pack the state into 3 chars per byte.
  pub fn pager_state(&self) -> Option<String> {
    self.paging_state
      .clone()
      .and_then(|c_bytes| match c_bytes.as_slice() {
        Some(u_vec) => {
          let cap = u_vec.len() * 3;

          let mut str_state: String = String::with_capacity(cap);

          for byte in u_vec {
            str_state.push_str(format!("{:03}", byte).as_str());
          }

          Some(str_state)
        }
        None => None
      })
  }
}

pub struct ExecPager<'a, P: 'a> {
  pager: &'a mut P,
  paging_state: Option<CBytes>,
  has_more_pages: Option<bool>,
  query: &'a PreparedQuery,
}

impl<
    'a,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    S: CDRSSession<'static, T, M>,
  > ExecPager<'a, SessionPager<'a, M, S, T>>
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
