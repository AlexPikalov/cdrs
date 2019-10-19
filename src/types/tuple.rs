use std::net::IpAddr;
use time::Timespec;
use uuid::Uuid;

use crate::error::{column_is_empty_err, Error, Result};
use crate::frame::frame_result::{CTuple, ColType, ColTypeOption, ColTypeOptionValue};
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::udt::UDT;
use crate::types::{ByIndex, CBytes, IntoRustByIndex};

use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct Tuple {
    data: Vec<(ColTypeOption, CBytes)>,
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Tuple) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        for (s, o) in self.data.iter().zip(other.data.iter()) {
            if s.1 != o.1 {
                return false;
            }
        }
        true
    }
}

impl Eq for Tuple {}

impl Hash for Tuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for data in &self.data {
            data.1.hash(state);
        }
    }
}

impl Tuple {
    pub fn new<'a>(data: Vec<CBytes>, metadata: &'a CTuple) -> Tuple {
        let meta_iter = metadata.types.iter();

        let acc = Vec::with_capacity(metadata.types.len());
        let d = meta_iter.zip(data.iter()).fold(acc, |mut a, v| {
            let (val_type, val_b) = v;
            a.push((val_type.clone(), val_b.clone()));
            a
        });

        Tuple { data: d }
    }
}

impl ByIndex for Tuple {}

into_rust_by_index!(Tuple, Blob);
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
into_rust_by_index!(Tuple, Decimal);
