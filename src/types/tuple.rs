use std::net::IpAddr;
use uuid::Uuid;
use time::Timespec;

use frame::frame_result::{ColTypeOption, CTuple, ColType, ColTypeOptionValue};
use types::{CBytes, IntoRustByIndex, ByIndex};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use types::udt::UDT;
use error::{Result, Error, column_is_empty_err};

#[derive(Debug)]
pub struct Tuple {
    data: Vec<(ColTypeOption, CBytes)>,
}

impl Tuple {
    pub fn new<'a>(data: Vec<CBytes>, metadata: &'a CTuple) -> Tuple {
        let meta_iter = metadata.types.iter();

        let acc = Vec::with_capacity(metadata.types.len());
        let d = meta_iter
            .zip(data.iter())
            .fold(acc, |mut a, v| {
                let (val_type, val_b) = v;
                a.push((val_type.clone(), val_b.clone()));
                a
            });

        Tuple { data: d }
    }
}

impl IntoRustByIndex<Vec<u8>> for Tuple {
    fn get_by_index(&self, index: usize) -> Result<Option<Vec<u8>>> {
        self.data
            .get(index)
            .ok_or(column_is_empty_err())
            .and_then(|v| {
                let &(ref col_type, ref bytes) = v;

                match col_type.id {
                    // XXX: unwrap Option
                    ColType::Blob => {
                        decode_blob(&bytes.as_plain().unwrap())
                            .map(Some)
                            .map_err(Into::into)
                    }
                    _ => Err(Error::General(format!("Cannot parse {:?} into Tuple ", col_type.id))),
                }
            })
    }
}

impl ByIndex for Tuple {}

into_rust_by_index!(Tuple, String);
into_rust_by_index!(Tuple, bool);
into_rust_by_index!(Tuple, i64);
into_rust_by_index!(Tuple, i32);
into_rust_by_index!(Tuple, i16);
into_rust_by_index!(Tuple, i8);
into_rust_by_index!(Tuple, f64);
into_rust_by_index!(Tuple, f32);
into_rust_by_index!(Tuple, IpAddr);
into_rust_by_index!(Tuple, Uuid);
into_rust_by_index!(Tuple, List);
into_rust_by_index!(Tuple, Map);
into_rust_by_index!(Tuple, UDT);
into_rust_by_index!(Tuple, Tuple);
into_rust_by_index!(Tuple, Timespec);
