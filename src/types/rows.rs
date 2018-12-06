use std::net::IpAddr;
use time::Timespec;
use uuid::Uuid;

use error::{column_is_empty_err, Error, Result};
use frame::frame_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata,
};
use types::blob::Blob;
use types::data_serialization_types::*;
use types::decimal::Decimal;
use types::list::List;
use types::map::Map;
use types::tuple::Tuple;
use types::udt::UDT;
use types::{ByIndex, ByName, CBytes, IntoRustByIndex, IntoRustByName};

#[derive(Clone, Debug)]
pub struct Row {
    metadata: RowsMetadata,
    row_content: Vec<CBytes>,
}

impl Row {
    pub fn from_frame_body(body: BodyResResultRows) -> Vec<Row> {
        body.rows_content
            .iter()
            .map(|row| Row {
                metadata: body.metadata.clone(),
                row_content: row.clone(),
            }).collect()
    }

    fn get_col_spec_by_name(&self, name: &str) -> Option<(&ColSpec, &CBytes)> {
        self.metadata
            .col_specs
            .iter()
            .position(|spec| spec.name.as_str() == name)
            .map(|i| {
                let ref col_spec = self.metadata.col_specs[i];
                let ref data = self.row_content[i];
                (col_spec, data)
            })
    }

    fn get_col_spec_by_index(&self, index: usize) -> Option<(&ColSpec, &CBytes)> {
        let specs = self.metadata.col_specs.iter();
        let values = self.row_content.iter();
        specs.zip(values).nth(index)
    }
}

impl ByName for Row {}

into_rust_by_name!(Row, Blob);
into_rust_by_name!(Row, String);
into_rust_by_name!(Row, bool);
into_rust_by_name!(Row, i64);
into_rust_by_name!(Row, i32);
into_rust_by_name!(Row, i16);
into_rust_by_name!(Row, i8);
into_rust_by_name!(Row, f64);
into_rust_by_name!(Row, f32);
into_rust_by_name!(Row, IpAddr);
into_rust_by_name!(Row, Uuid);
into_rust_by_name!(Row, List);
into_rust_by_name!(Row, Map);
into_rust_by_name!(Row, UDT);
into_rust_by_name!(Row, Tuple);
into_rust_by_name!(Row, Timespec);
into_rust_by_name!(Row, Decimal);

impl ByIndex for Row {}

into_rust_by_index!(Row, Blob);
into_rust_by_index!(Row, String);
into_rust_by_index!(Row, bool);
into_rust_by_index!(Row, i64);
into_rust_by_index!(Row, i32);
into_rust_by_index!(Row, i16);
into_rust_by_index!(Row, i8);
into_rust_by_index!(Row, f64);
into_rust_by_index!(Row, f32);
into_rust_by_index!(Row, IpAddr);
into_rust_by_index!(Row, Uuid);
into_rust_by_index!(Row, List);
into_rust_by_index!(Row, Map);
into_rust_by_index!(Row, UDT);
into_rust_by_index!(Row, Tuple);
into_rust_by_index!(Row, Timespec);
into_rust_by_index!(Row, Decimal);
