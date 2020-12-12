use r2d2;
use std::cell::RefCell;
use std::marker::PhantomData;

use crate::cluster::CDRSSession;
use crate::consistency::Consistency;
use crate::error;
use crate::frame::frame_result::{RowsMetadata, RowsMetadataFlag};
use crate::query::{PreparedQuery, QueryParams, QueryParamsBuilder, QueryValues};
use crate::transport::CDRSTransport;
use crate::types::rows::Row;
use crate::types::CBytes;

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

    pub fn query_with_pager_state<Q>(
        &'a mut self,
        query: Q,
        state: PagerState,
    ) -> QueryPager<'a, Q, SessionPager<'a, M, S, T>>
    where
        Q: ToString,
    {
        QueryPager {
            pager: self,
            pager_state: state,
            query,
            qv: None,
            consistency: Consistency::One,
        }
    }

    pub fn query_with_pager_state_params<Q>(
        &'a mut self,
        query: Q,
        state: PagerState,
        qp: QueryParams,
    ) -> QueryPager<'a, Q, SessionPager<'a, M, S, T>>
    where
        Q: ToString,
    {
        QueryPager {
            pager: self,
            pager_state: state,
            query,
            qv: qp.values,
            consistency: qp.consistency,
        }
    }

    pub fn query<Q>(&'a mut self, query: Q) -> QueryPager<'a, Q, SessionPager<'a, M, S, T>>
    where
        Q: ToString,
    {
        self.query_with_param(
            query,
            QueryParamsBuilder::new()
                .consistency(Consistency::One)
                .finalize(),
        )
    }

    pub fn query_with_param<Q>(
        &'a mut self,
        query: Q,
        qp: QueryParams,
    ) -> QueryPager<'a, Q, SessionPager<'a, M, S, T>>
    where
        Q: ToString,
    {
        self.query_with_pager_state_params(query, PagerState::new(), qp)
    }

    pub fn exec_with_pager_state(
        &'a mut self,
        query: &'a PreparedQuery,
        state: PagerState,
    ) -> ExecPager<'a, SessionPager<'a, M, S, T>> {
        ExecPager {
            pager: self,
            pager_state: state,
            query,
        }
    }

    pub fn exec(
        &'a mut self,
        query: &'a PreparedQuery,
    ) -> ExecPager<'a, SessionPager<'a, M, S, T>> {
        self.exec_with_pager_state(query, PagerState::new())
    }
}

pub struct QueryPager<'a, Q: ToString, P: 'a> {
    pager: &'a mut P,
    pager_state: PagerState,
    query: Q,
    qv: Option<QueryValues>,
    consistency: Consistency,
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
        let mut params = QueryParamsBuilder::new()
            .consistency(self.consistency)
            .page_size(self.pager.page_size);

        if let Some(qv) = &self.qv {
            params = params.values(qv.clone());
        }
        if let Some(cursor) = &self.pager_state.cursor {
            params = params.paging_state(cursor.clone());
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

        self.pager_state.has_more_pages =
            Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()));
        self.pager_state.cursor = metadata.paging_state.clone();
        body.into_rows()
            .ok_or("Pager query should yield a vector of rows".into())
    }

    pub fn has_more(&self) -> bool {
        self.pager_state.has_more_pages.unwrap_or(false)
    }

    /// This method returns a copy of pager state so
    /// the state may be used later for continuing paging.
    pub fn pager_state(&self) -> PagerState {
        self.pager_state.clone()
    }
}

pub struct ExecPager<'a, P: 'a> {
    pager: &'a mut P,
    pager_state: PagerState,
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
        if self.pager_state.cursor.is_some() {
            params = params.paging_state(self.pager_state.cursor.clone().unwrap());
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

        self.pager_state.has_more_pages =
            Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()));
        self.pager_state.cursor = metadata.paging_state.clone();
        body.into_rows()
            .ok_or("Pager query should yield a vector of rows".into())
    }

    pub fn has_more(&self) -> bool {
        self.pager_state.has_more_pages.unwrap_or(false)
    }

    /// This method returns a copy of pager state so
    /// the state may be used later for continuing paging.
    pub fn pager_state(&self) -> PagerState {
        self.pager_state.clone()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct PagerState {
    cursor: Option<CBytes>,
    has_more_pages: Option<bool>,
}

impl PagerState {
    pub fn new() -> Self {
        PagerState {
            cursor: None,
            has_more_pages: None,
        }
    }

    pub fn with_cursor(cursor: CBytes) -> Self {
        PagerState {
            cursor: Some(cursor),
            has_more_pages: None,
        }
    }

    pub fn with_cursor_and_more_flag(cursor: CBytes, has_more: bool) -> Self {
        PagerState {
            cursor: Some(cursor),
            has_more_pages: Some(has_more),
        }
    }

    pub fn has_more(&self) -> bool {
        self.has_more_pages.unwrap_or(false)
    }

    pub fn get_cursor(&self) -> Option<CBytes> {
        self.cursor.clone()
    }
}
