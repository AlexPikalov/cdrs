use std::net;
use frame::frame_result::{ColSpec, ColType, ColTypeOptionValue};
use types::{CBytes, AsRust};
use types::data_serialization_types::*;


pub struct List {
    /// column spec of the list, i.e. id should be List as it's a list and value should contain
    /// a type of list items.
    metadata: ColSpec,
    data: Vec<CBytes>
}

impl List {
    pub fn new(data: Vec<CBytes>, metadata: ColSpec) -> List {
        return List {
            metadata: metadata,
            data: data
        };
    }
}

impl AsRust<Vec<Vec<u8>>> for List {
    /// Converts cassandra list of blobs into Rust `Vec<Vec<u8>>`
    fn as_rust(&self) -> Option<Vec<Vec<u8>>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Blob => Some(self.data
                        .iter()
                        .map(|bytes| decode_blob(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<String>> for List {
    /// Converts cassandra list of String-like values into Rust `Vec<String>`
    fn as_rust(&self) -> Option<Vec<String>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Custom => Some(self.data
                        .iter()
                        .map(|bytes| decode_custom(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Ascii => Some(self.data
                        .iter()
                        .map(|bytes| decode_ascii(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Varchar => Some(self.data
                        .iter()
                        .map(|bytes| decode_varchar(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<bool>> for List {
    /// Converts cassandra list of boolean-like values into Rust `Vec<bool>`
    fn as_rust(&self) -> Option<Vec<bool>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Boolean => Some(self.data
                        .iter()
                        .map(|bytes| decode_boolean(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<i64>> for List {
    /// Converts cassandra list of i64-like values into Rust `Vec<i64>`
    fn as_rust(&self) -> Option<Vec<i64>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Bigint => Some(self.data
                        .iter()
                        .map(|bytes| decode_bigint(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Timestamp => Some(self.data
                        .iter()
                        .map(|bytes| decode_timestamp(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Time => Some(self.data
                        .iter()
                        .map(|bytes| decode_time(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Varint => Some(self.data
                        .iter()
                        .map(|bytes| decode_varint(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<i32>> for List {
    /// Converts cassandra list of i32-like values into Rust `Vec<i32>`
    fn as_rust(&self) -> Option<Vec<i32>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Int => Some(self.data
                        .iter()
                        .map(|bytes| decode_int(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Date => Some(self.data
                        .iter()
                        .map(|bytes| decode_date(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<i16>> for List {
    /// Converts cassandra list of i16-like values into Rust `Vec<i16>`
    fn as_rust(&self) -> Option<Vec<i16>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Smallint => Some(self.data
                        .iter()
                        .map(|bytes| decode_smallint(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<f64>> for List {
    /// Converts cassandra list of f64-like values into Rust `Vec<f64>`
    fn as_rust(&self) -> Option<Vec<f64>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Double => Some(self.data
                        .iter()
                        .map(|bytes| decode_double(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<f32>> for List {
    /// Converts cassandra list of f32-like values into Rust `Vec<f32>`
    fn as_rust(&self) -> Option<Vec<f32>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Decimal => Some(self.data
                        .iter()
                        .map(|bytes| decode_decimal(bytes.as_plain()).unwrap())
                        .collect()),
                    ColType::Float => Some(self.data
                        .iter()
                        .map(|bytes| decode_float(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<Vec<net::IpAddr>> for List {
    /// Converts cassandra list of Inet values into Rust `Vec<net::IpAddr>`
    fn as_rust(&self) -> Option<Vec<net::IpAddr>> {
        if self.metadata.col_type.value.is_none() {
            return None;
        }

        match self.metadata.col_type.value.clone().unwrap() {
            ColTypeOptionValue::CList(ref type_option) => {
                match type_option.id {
                    ColType::Inet => Some(self.data
                        .iter()
                        .map(|bytes| decode_inet(bytes.as_plain()).unwrap())
                        .collect()),
                    _ => None
                }
            },
            _ => None
        }
    }
}
