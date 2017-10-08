use std::net::IpAddr;
use uuid::Uuid;
use time::Timespec;

use frame::frame_result::{ColTypeOption, CTuple, ColType, ColTypeOptionValue};
use types::{CBytes, IntoRustByIndex, ByIndex};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use types::udt::UDT;
use types::blob::Blob;
use error::{Result, Error, column_is_empty_err};

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
