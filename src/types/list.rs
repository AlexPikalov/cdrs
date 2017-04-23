use std::net::IpAddr;
use uuid::Uuid;
use frame::frame_result::{ColType, ColTypeOptionValue, ColTypeOption};
use types::{CBytes, AsRustType, AsRust};
use types::data_serialization_types::*;
use types::map::Map;
use types::udt::UDT;
use error::{Result, Error};

// TODO: consider using pointers to ColTypeOption and Vec<CBytes> instead of owning them.
#[derive(Debug)]
pub struct List {
    /// column spec of the list, i.e. id should be List as it's a list and value should contain
    /// a type of list items.
    metadata: ColTypeOption,
    data: Vec<CBytes>,
}

impl List {
    pub fn new(data: Vec<CBytes>, metadata: ColTypeOption) -> List {
        List {
            metadata: metadata,
            data: data,
        }
    }

    fn map<T, F>(&self, f: F) -> Vec<T>
        where F: FnMut(&CBytes) -> T
    {
        self.data.iter().map(f).collect()
    }
}

impl AsRust for List {}

impl AsRustType<Vec<Vec<u8>>> for List {
    /// Converts cassandra list of blobs into Rust `Vec<Vec<u8>>`
    fn as_rust_type(&self) -> Result<Vec<Vec<u8>>> {
        match self.metadata.value {
            Some(ColTypeOptionValue::CList(ref type_option)) => {
                match type_option.id {
                    // XXX unwrap
                    ColType::Blob => Ok(self.map(|bytes| decode_blob(bytes.as_plain()).unwrap())),
                    _ => unreachable!(),
                }
            }
            Some(ColTypeOptionValue::CSet(ref type_option)) => {
                match type_option.id {
                    ColType::Blob => Ok(self.map(|bytes| decode_blob(bytes.as_plain()).unwrap())),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

list_as_rust!(String);
list_as_rust!(bool);
list_as_rust!(i64);
list_as_rust!(i32);
list_as_rust!(i16);
list_as_rust!(i8);
list_as_rust!(f64);
list_as_rust!(f32);
list_as_rust!(IpAddr);
list_as_rust!(Uuid);
list_as_rust!(List);
list_as_rust!(Map);
list_as_rust!(UDT);
