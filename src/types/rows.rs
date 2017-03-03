use std::net::IpAddr;
use uuid::Uuid;

use frame::frame_result::{RowsMetadata, ColType, ColSpec, BodyResResultRows, ColTypeOption,
                          ColTypeOptionValue};
use types::{CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use types::udt::UDT;
use error::{Error, Result, column_is_empty_err};

#[derive(Debug)]
pub struct Row {
    metadata: RowsMetadata,
    row_content: Vec<CBytes>,
}

impl Row {
    pub fn from_frame_body(body: BodyResResultRows) -> Vec<Row> {
        body.rows_content
            .iter()
            .map(|row| {
                Row {
                    metadata: body.metadata.clone(),
                    row_content: row.clone(),
                }
            })
            .collect()
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
}

impl IntoRustByName<Vec<u8>> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<Vec<u8>>> {
        self.get_col_spec_by_name(name)
            .map(|(col_spec, cbytes)| {
                let ref col_type = col_spec.col_type;
                as_rust!(col_type, cbytes, Vec<u8>)
            })
    }
}

macro_rules! row_into_rust_by_name {
    ($($into_type:tt)*) => (
        impl IntoRustByName<$($into_type)*> for Row {
            fn get_by_name(&self, name: &str) -> Option<Result<$($into_type)*>> {
                self.get_col_spec_by_name(name)
                    .map(|(col_spec, cbytes)| {
                        if cbytes.is_empty() {
                            return Err(column_is_empty_err());
                        }

                        let ref col_type = col_spec.col_type;
                        as_rust!(col_type, cbytes, $($into_type)*)
                    })
            }
        }
    );
}

row_into_rust_by_name!(String);
row_into_rust_by_name!(bool);
row_into_rust_by_name!(i64);
row_into_rust_by_name!(i32);
row_into_rust_by_name!(i16);
row_into_rust_by_name!(i8);
row_into_rust_by_name!(f64);
row_into_rust_by_name!(f32);
row_into_rust_by_name!(IpAddr);
row_into_rust_by_name!(Uuid);
row_into_rust_by_name!(List);
row_into_rust_by_name!(Map);
row_into_rust_by_name!(UDT);
