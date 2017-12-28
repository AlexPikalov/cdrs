use std::net::IpAddr;
use std::collections::HashMap;
use uuid::Uuid;
use time::Timespec;

use frame::frame_result::{CUdt, ColType, ColTypeOption, ColTypeOptionValue};
use types::{ByName, CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use types::tuple::Tuple;
use types::blob::Blob;
use error::{column_is_empty_err, Error, Result};

#[derive(Debug)]
pub struct UDT {
    data: HashMap<String, (ColTypeOption, CBytes)>,
}

impl UDT {
    pub fn new<'a>(data: Vec<CBytes>, metadata: &'a CUdt) -> UDT {
        let meta_iter = metadata.descriptions.iter();

        let acc: HashMap<String, (ColTypeOption, CBytes)> =
            HashMap::with_capacity(metadata.descriptions.len());
        let d = meta_iter.zip(data.iter()).fold(acc, |mut a, v| {
            let (m, val_b) = v;
            let &(ref name_b, ref val_type) = m;
            let name = name_b.as_plain();
            a.insert(name, (val_type.clone(), val_b.clone()));
            a
        });

        UDT { data: d }
    }
}

impl ByName for UDT {}

into_rust_by_name!(UDT, Blob);
into_rust_by_name!(UDT, String);
into_rust_by_name!(UDT, bool);
into_rust_by_name!(UDT, i64);
into_rust_by_name!(UDT, i32);
into_rust_by_name!(UDT, i16);
into_rust_by_name!(UDT, i8);
into_rust_by_name!(UDT, f64);
into_rust_by_name!(UDT, f32);
into_rust_by_name!(UDT, IpAddr);
into_rust_by_name!(UDT, Uuid);
into_rust_by_name!(UDT, List);
into_rust_by_name!(UDT, Map);
into_rust_by_name!(UDT, UDT);
into_rust_by_name!(UDT, Tuple);
into_rust_by_name!(UDT, Timespec);
