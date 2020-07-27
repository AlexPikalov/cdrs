use std::collections::HashMap;
use std::net::IpAddr;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::frame::frame_result::{ColType, ColTypeOption, ColTypeOptionValue};
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::list::List;
use crate::types::tuple::Tuple;
use crate::types::udt::UDT;
use crate::types::{AsRust, AsRustType, CBytes};

#[derive(Debug)]
pub struct Map {
    metadata: ColTypeOption,
    data: Vec<(CBytes, CBytes)>,
}

impl Map {
    /// Creates new `Map` using the provided data and key and value types.
    pub fn new(data: Vec<(CBytes, CBytes)>, meta: ColTypeOption) -> Map {
        Map {
            metadata: meta,
            data: data,
        }
    }
}

impl AsRust for Map {}

// Generate `AsRustType` implementations for all kinds of map types.
// The macro `map_as_rust!` takes the key and value types as lists of token trees.
// This is needed because `as_rust_type!` is called by `map_as_rust!`.
// In order to distinguish the key and value types, they are enclosed by curly braces.

map_as_rust!({ Blob }, { Blob });
map_as_rust!({ Blob }, { String });
map_as_rust!({ Blob }, { bool });
map_as_rust!({ Blob }, { i64 });
map_as_rust!({ Blob }, { i32 });
map_as_rust!({ Blob }, { i16 });
map_as_rust!({ Blob }, { i8 });
map_as_rust!({ Blob }, { f64 });
map_as_rust!({ Blob }, { f32 });
map_as_rust!({ Blob }, { IpAddr });
map_as_rust!({ Blob }, { Uuid });
map_as_rust!({ Blob }, { PrimitiveDateTime });
map_as_rust!({ Blob }, { List });
map_as_rust!({ Blob }, { Map });
map_as_rust!({ Blob }, { UDT });
map_as_rust!({ Blob }, { Tuple });
map_as_rust!({ Blob }, { Decimal });

map_as_rust!({ String }, { Blob });
map_as_rust!({ String }, { String });
map_as_rust!({ String }, { bool });
map_as_rust!({ String }, { i64 });
map_as_rust!({ String }, { i32 });
map_as_rust!({ String }, { i16 });
map_as_rust!({ String }, { i8 });
map_as_rust!({ String }, { f64 });
map_as_rust!({ String }, { f32 });
map_as_rust!({ String }, { IpAddr });
map_as_rust!({ String }, { Uuid });
map_as_rust!({ String }, { PrimitiveDateTime });
map_as_rust!({ String }, { List });
map_as_rust!({ String }, { Map });
map_as_rust!({ String }, { UDT });
map_as_rust!({ String }, { Tuple });
map_as_rust!({ String }, { Decimal });

map_as_rust!({ bool }, { Blob });
map_as_rust!({ bool }, { String });
map_as_rust!({ bool }, { bool });
map_as_rust!({ bool }, { i64 });
map_as_rust!({ bool }, { i32 });
map_as_rust!({ bool }, { i16 });
map_as_rust!({ bool }, { i8 });
map_as_rust!({ bool }, { f64 });
map_as_rust!({ bool }, { f32 });
map_as_rust!({ bool }, { IpAddr });
map_as_rust!({ bool }, { Uuid });
map_as_rust!({ bool }, { PrimitiveDateTime });
map_as_rust!({ bool }, { List });
map_as_rust!({ bool }, { Map });
map_as_rust!({ bool }, { UDT });
map_as_rust!({ bool }, { Tuple });
map_as_rust!({ bool }, { Decimal });

map_as_rust!({ i64 }, { Blob });
map_as_rust!({ i64 }, { String });
map_as_rust!({ i64 }, { bool });
map_as_rust!({ i64 }, { i64 });
map_as_rust!({ i64 }, { i32 });
map_as_rust!({ i64 }, { i16 });
map_as_rust!({ i64 }, { i8 });
map_as_rust!({ i64 }, { f64 });
map_as_rust!({ i64 }, { f32 });
map_as_rust!({ i64 }, { IpAddr });
map_as_rust!({ i64 }, { Uuid });
map_as_rust!({ i64 }, { PrimitiveDateTime });
map_as_rust!({ i64 }, { List });
map_as_rust!({ i64 }, { Map });
map_as_rust!({ i64 }, { UDT });
map_as_rust!({ i64 }, { Tuple });
map_as_rust!({ i64 }, { Decimal });

map_as_rust!({ i32 }, { Blob });
map_as_rust!({ i32 }, { String });
map_as_rust!({ i32 }, { bool });
map_as_rust!({ i32 }, { i64 });
map_as_rust!({ i32 }, { i32 });
map_as_rust!({ i32 }, { i16 });
map_as_rust!({ i32 }, { i8 });
map_as_rust!({ i32 }, { f64 });
map_as_rust!({ i32 }, { f32 });
map_as_rust!({ i32 }, { IpAddr });
map_as_rust!({ i32 }, { Uuid });
map_as_rust!({ i32 }, { PrimitiveDateTime });
map_as_rust!({ i32 }, { List });
map_as_rust!({ i32 }, { Map });
map_as_rust!({ i32 }, { UDT });
map_as_rust!({ i32 }, { Tuple });
map_as_rust!({ i32 }, { Decimal });

map_as_rust!({ i16 }, { Blob });
map_as_rust!({ i16 }, { String });
map_as_rust!({ i16 }, { bool });
map_as_rust!({ i16 }, { i64 });
map_as_rust!({ i16 }, { i32 });
map_as_rust!({ i16 }, { i16 });
map_as_rust!({ i16 }, { i8 });
map_as_rust!({ i16 }, { f64 });
map_as_rust!({ i16 }, { f32 });
map_as_rust!({ i16 }, { IpAddr });
map_as_rust!({ i16 }, { Uuid });
map_as_rust!({ i16 }, { PrimitiveDateTime });
map_as_rust!({ i16 }, { List });
map_as_rust!({ i16 }, { Map });
map_as_rust!({ i16 }, { UDT });
map_as_rust!({ i16 }, { Tuple });
map_as_rust!({ i16 }, { Decimal });

map_as_rust!({ i8 }, { Blob });
map_as_rust!({ i8 }, { String });
map_as_rust!({ i8 }, { bool });
map_as_rust!({ i8 }, { i64 });
map_as_rust!({ i8 }, { i32 });
map_as_rust!({ i8 }, { i16 });
map_as_rust!({ i8 }, { i8 });
map_as_rust!({ i8 }, { f64 });
map_as_rust!({ i8 }, { f32 });
map_as_rust!({ i8 }, { IpAddr });
map_as_rust!({ i8 }, { Uuid });
map_as_rust!({ i8 }, { PrimitiveDateTime });
map_as_rust!({ i8 }, { List });
map_as_rust!({ i8 }, { Map });
map_as_rust!({ i8 }, { UDT });
map_as_rust!({ i8 }, { Tuple });
map_as_rust!({ i8 }, { Decimal });

map_as_rust!({ IpAddr }, { Blob });
map_as_rust!({ IpAddr }, { String });
map_as_rust!({ IpAddr }, { bool });
map_as_rust!({ IpAddr }, { i64 });
map_as_rust!({ IpAddr }, { i32 });
map_as_rust!({ IpAddr }, { i16 });
map_as_rust!({ IpAddr }, { i8 });
map_as_rust!({ IpAddr }, { f64 });
map_as_rust!({ IpAddr }, { f32 });
map_as_rust!({ IpAddr }, { IpAddr });
map_as_rust!({ IpAddr }, { Uuid });
map_as_rust!({ IpAddr }, { PrimitiveDateTime });
map_as_rust!({ IpAddr }, { List });
map_as_rust!({ IpAddr }, { Map });
map_as_rust!({ IpAddr }, { UDT });
map_as_rust!({ IpAddr }, { Tuple });
map_as_rust!({ IpAddr }, { Decimal });

map_as_rust!({ Uuid }, { Blob });
map_as_rust!({ Uuid }, { String });
map_as_rust!({ Uuid }, { bool });
map_as_rust!({ Uuid }, { i64 });
map_as_rust!({ Uuid }, { i32 });
map_as_rust!({ Uuid }, { i16 });
map_as_rust!({ Uuid }, { i8 });
map_as_rust!({ Uuid }, { f64 });
map_as_rust!({ Uuid }, { f32 });
map_as_rust!({ Uuid }, { IpAddr });
map_as_rust!({ Uuid }, { Uuid });
map_as_rust!({ Uuid }, { PrimitiveDateTime });
map_as_rust!({ Uuid }, { List });
map_as_rust!({ Uuid }, { Map });
map_as_rust!({ Uuid }, { UDT });
map_as_rust!({ Uuid }, { Tuple });
map_as_rust!({ Uuid }, { Decimal });

map_as_rust!({ PrimitiveDateTime }, { Blob });
map_as_rust!({ PrimitiveDateTime }, { String });
map_as_rust!({ PrimitiveDateTime }, { bool });
map_as_rust!({ PrimitiveDateTime }, { i64 });
map_as_rust!({ PrimitiveDateTime }, { i32 });
map_as_rust!({ PrimitiveDateTime }, { i16 });
map_as_rust!({ PrimitiveDateTime }, { i8 });
map_as_rust!({ PrimitiveDateTime }, { f64 });
map_as_rust!({ PrimitiveDateTime }, { f32 });
map_as_rust!({ PrimitiveDateTime }, { IpAddr });
map_as_rust!({ PrimitiveDateTime }, { Uuid });
map_as_rust!({ PrimitiveDateTime }, { PrimitiveDateTime });
map_as_rust!({ PrimitiveDateTime }, { List });
map_as_rust!({ PrimitiveDateTime }, { Map });
map_as_rust!({ PrimitiveDateTime }, { UDT });
map_as_rust!({ PrimitiveDateTime }, { Tuple });
map_as_rust!({ PrimitiveDateTime }, { Decimal });

map_as_rust!({ Tuple }, { Blob });
map_as_rust!({ Tuple }, { String });
map_as_rust!({ Tuple }, { bool });
map_as_rust!({ Tuple }, { i64 });
map_as_rust!({ Tuple }, { i32 });
map_as_rust!({ Tuple }, { i16 });
map_as_rust!({ Tuple }, { i8 });
map_as_rust!({ Tuple }, { f64 });
map_as_rust!({ Tuple }, { f32 });
map_as_rust!({ Tuple }, { IpAddr });
map_as_rust!({ Tuple }, { Uuid });
map_as_rust!({ Tuple }, { PrimitiveDateTime });
map_as_rust!({ Tuple }, { List });
map_as_rust!({ Tuple }, { Map });
map_as_rust!({ Tuple }, { UDT });
map_as_rust!({ Tuple }, { Tuple });
map_as_rust!({ Tuple }, { Decimal });
