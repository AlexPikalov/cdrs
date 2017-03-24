use std::net::IpAddr;
use std::collections::HashMap;
use uuid::Uuid;

use frame::frame_result::{ColTypeOption, CUdt, ColType, ColTypeOptionValue};
use types::{CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use error::{Result, Error, column_is_empty_err};

#[derive(Debug)]
pub struct UDT {
    data: HashMap<String, (ColTypeOption, CBytes)>,
}

impl UDT {
    pub fn new<'a>(data: Vec<CBytes>, metadata: &'a CUdt) -> UDT {
        let meta_iter = metadata.descriptions.iter();

        let acc: HashMap<String, (ColTypeOption, CBytes)> =
            HashMap::with_capacity(metadata.descriptions.len());
        let d = meta_iter.zip(data.iter())
            .fold(acc, |mut a, v| {
                let (m, val_b) = v;
                let &(ref name_b, ref val_type) = m;
                let name = name_b.as_plain();
                a.insert(name, (val_type.clone(), val_b.clone()));
                a
            });

         UDT { data: d }
    }
}

impl IntoRustByName<Vec<u8>> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<Vec<u8>>> {
         self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;

            match col_type.id {
                ColType::Blob => decode_blob(bytes.as_plain()).map_err(|err| err.into()),
                _ => Err(Error::General(format!("Cannot parse  {:?} into UDT ", col_type.id))),
            }
        })
    }
}

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
