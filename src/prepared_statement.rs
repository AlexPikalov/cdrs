
use frame::{Frame, Flag};
use IntoBytes;
use frame::parser::parse_frame;
use authenticators::Authenticator;
use error;
use transport::CDRSTransport;
use consistency::Consistency;
use client::Session;
use query::QueryParamsBuilder;
use types::value::Value;
use frame::frame_response::ResponseBody;
use std::collections::BTreeMap;
use std::convert::Into;

#[derive(Debug)]
pub struct PreparedStatement {
    tracing: Option<bool>,
    warnings: Option<bool>,
    consistency: Consistency,
    with_names: bool,
    page_size: Option<i32>,
    paging_state: Option<i32>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
    frame: Option<Frame>,
    query_markers: BTreeMap<u32, Value>,
}

impl Default for PreparedStatement {
    fn default() -> PreparedStatement {
        let query_markers = BTreeMap::new();

        PreparedStatement {
            tracing: None,
            warnings: None,
            consistency: Consistency::One,
            with_names: false,
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            timestamp: None,
            frame: None,
            query_markers: query_markers,
        }
    }
}

impl PreparedStatement {
    pub fn set_string(&mut self, index: u32, val: &'static str) ->error::Result<()>{
        self.query_markers.insert(index, val.to_string().into());
        Ok(())

    }
}

/**
Prepare and execute statement with prepared Statement
*/

pub trait PrepareAndExecute {
    fn prepare_statement(&mut self,
                         query: String,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<PreparedStatement>;

    /// id of prepared request
    ///pub id: CBytesShort,
    /// metadata
    ///pub metadata: PreparedMetadata,
    /// It is defined exactly the same as <metadata> in the Rows
    /// documentation.
    ///pub result_metadata: RowsMetadata,
    fn execute_statement(&mut self, prepared_statement: PreparedStatement) -> error::Result<Frame>;
}


impl<'a, T: Authenticator + 'a, X: CDRSTransport + 'a> PrepareAndExecute for Session<T, X> {
    fn prepare_statement(&mut self,
                         query: String,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<PreparedStatement> {
        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let options_frame = Frame::new_req_prepare(query, flags).into_cbytes();

        (self.cdrs.transport.write(options_frame.as_slice()))?;

        parse_frame(&mut self.cdrs.transport, &self.compressor).map(|frame| {
            PreparedStatement {
                consistency: Consistency::One,
                frame: Some(frame),
                ..Default::default()
            }
        })
    }

    fn execute_statement(&mut self, prepared_statement: PreparedStatement) -> error::Result<Frame> {
        let mut flags = vec![];
        if let Some(tracing) = prepared_statement.tracing {
            if tracing {
                flags.push(Flag::Tracing);
            }
        };

        if let Some(with_warnings) = prepared_statement.warnings {
            if with_warnings {
                flags.push(Flag::Warning);
            }
        };
        if let Some(framed) = prepared_statement.frame {
            match framed.get_body() {
                ResponseBody::Result(res) => {
                    //Prepared(BodyResResultPrepared),
                    if let Some(body_res_result_prepared) = res.into_prepared() {

                        let v: Vec<Value> =
                            prepared_statement.query_markers.values().cloned().collect();
                        let query_parameters =
                            QueryParamsBuilder::new(prepared_statement.consistency)
                                .values(v)
                                .finalize();
                        let options_frame = Frame::new_req_execute(&body_res_result_prepared.id,
                                                                   query_parameters,
                                                                   flags)
                            .into_cbytes();
                        (self.cdrs.transport.write(options_frame.as_slice()))?;
                        return parse_frame(&mut self.cdrs.transport, &self.compressor);

                    } else {
                        return Err(error::Error::from("no bodyResResultPrepared".to_string()));
                    }
                }
                _ => return Err(error::Error::from("no body".to_string())),
            }
        } else {
            Err(error::Error::from("no frame given to input".to_string()))

        }


    }
}
