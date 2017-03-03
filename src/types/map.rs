use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;

use types::{AsRust, CBytes};
use frame::frame_result::{ColTypeOption, ColTypeOptionValue, ColType};
use types::data_serialization_types::*;
use types::list::List;
use types::udt::UDT;
use error::{Error, Result};

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

macro_rules! map_as_rust {
    ($(K $key_type:tt)*, $(V $val_type:tt)*) => (
        impl AsRust<HashMap<$($key_type)*, $($val_type)*>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust(&self) -> Result<HashMap<$($key_type)*, $($val_type)*>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CMap((ref key_type_option, ref val_type_option))) => {
                        let mut map = HashMap::with_capacity(self.data.len());

                        for &(ref key, ref val) in self.data.iter() {
                            let key_type_option = key_type_option.clone();
                            let val_type_option = val_type_option.clone();
                            let key = as_rust!(key_type_option, key, $($key_type)*);
                            let val = as_rust!(val_type_option, val, $($val_type)*);
                            map.insert(key, val);
                        }

                        Ok(map)
                    }
                    _ => unreachable!()
                }
            }
        }
    );
}

// Generate `AsRust` implementations for all kinds of map types.
// The macro `map_as_rust!` takes the types as lists of token trees,
// which means that for example the type definition of `Vec<u8>` is split into
// four tokens `Vec`, `<`, `u8` and `>`. Since `map_as_rust!` takes two lists
// of token trees in a row, they have to be separated by a prefix.
// So every token of the key type has to prefixed with a `K` and the value tokens with a `V`.

map_as_rust!(K Vec K < K u8 K >, V Vec V < V u8 V >);
map_as_rust!(K Vec K < K u8 K >, V String);
map_as_rust!(K Vec K < K u8 K >, V bool);
map_as_rust!(K Vec K < K u8 K >, V i64);
map_as_rust!(K Vec K < K u8 K >, V i32);
map_as_rust!(K Vec K < K u8 K >, V i16);
map_as_rust!(K Vec K < K u8 K >, V i8);
map_as_rust!(K Vec K < K u8 K >, V f64);
map_as_rust!(K Vec K < K u8 K >, V f32);
map_as_rust!(K Vec K < K u8 K >, V IpAddr);
map_as_rust!(K Vec K < K u8 K >, V Uuid);
map_as_rust!(K Vec K < K u8 K >, V List);
map_as_rust!(K Vec K < K u8 K >, V Map);
map_as_rust!(K Vec K < K u8 K >, V UDT);

map_as_rust!(K String, V Vec V < V u8 V >);
map_as_rust!(K String, V String);
map_as_rust!(K String, V bool);
map_as_rust!(K String, V i64);
map_as_rust!(K String, V i32);
map_as_rust!(K String, V i16);
map_as_rust!(K String, V i8);
map_as_rust!(K String, V f64);
map_as_rust!(K String, V f32);
map_as_rust!(K String, V IpAddr);
map_as_rust!(K String, V Uuid);
map_as_rust!(K String, V List);
map_as_rust!(K String, V Map);
map_as_rust!(K String, V UDT);

map_as_rust!(K bool, V Vec V < V u8 V >);
map_as_rust!(K bool, V String);
map_as_rust!(K bool, V bool);
map_as_rust!(K bool, V i64);
map_as_rust!(K bool, V i32);
map_as_rust!(K bool, V i16);
map_as_rust!(K bool, V i8);
map_as_rust!(K bool, V f64);
map_as_rust!(K bool, V f32);
map_as_rust!(K bool, V IpAddr);
map_as_rust!(K bool, V Uuid);
map_as_rust!(K bool, V List);
map_as_rust!(K bool, V Map);
map_as_rust!(K bool, V UDT);

map_as_rust!(K i64, V Vec V < V u8 V >);
map_as_rust!(K i64, V String);
map_as_rust!(K i64, V bool);
map_as_rust!(K i64, V i64);
map_as_rust!(K i64, V i32);
map_as_rust!(K i64, V i16);
map_as_rust!(K i64, V i8);
map_as_rust!(K i64, V f64);
map_as_rust!(K i64, V f32);
map_as_rust!(K i64, V IpAddr);
map_as_rust!(K i64, V Uuid);
map_as_rust!(K i64, V List);
map_as_rust!(K i64, V Map);
map_as_rust!(K i64, V UDT);

map_as_rust!(K i32, V Vec V < V u8 V >);
map_as_rust!(K i32, V String);
map_as_rust!(K i32, V bool);
map_as_rust!(K i32, V i64);
map_as_rust!(K i32, V i32);
map_as_rust!(K i32, V i16);
map_as_rust!(K i32, V i8);
map_as_rust!(K i32, V f64);
map_as_rust!(K i32, V f32);
map_as_rust!(K i32, V IpAddr);
map_as_rust!(K i32, V Uuid);
map_as_rust!(K i32, V List);
map_as_rust!(K i32, V Map);
map_as_rust!(K i32, V UDT);

map_as_rust!(K i16, V Vec V < V u8 V >);
map_as_rust!(K i16, V String);
map_as_rust!(K i16, V bool);
map_as_rust!(K i16, V i64);
map_as_rust!(K i16, V i32);
map_as_rust!(K i16, V i16);
map_as_rust!(K i16, V i8);
map_as_rust!(K i16, V f64);
map_as_rust!(K i16, V f32);
map_as_rust!(K i16, V IpAddr);
map_as_rust!(K i16, V Uuid);
map_as_rust!(K i16, V List);
map_as_rust!(K i16, V Map);
map_as_rust!(K i16, V UDT);

map_as_rust!(K i8, V Vec V < V u8 V >);
map_as_rust!(K i8, V String);
map_as_rust!(K i8, V bool);
map_as_rust!(K i8, V i64);
map_as_rust!(K i8, V i32);
map_as_rust!(K i8, V i16);
map_as_rust!(K i8, V i8);
map_as_rust!(K i8, V f64);
map_as_rust!(K i8, V f32);
map_as_rust!(K i8, V IpAddr);
map_as_rust!(K i8, V Uuid);
map_as_rust!(K i8, V List);
map_as_rust!(K i8, V Map);
map_as_rust!(K i8, V UDT);

map_as_rust!(K IpAddr, V Vec V < V u8 V >);
map_as_rust!(K IpAddr, V String);
map_as_rust!(K IpAddr, V bool);
map_as_rust!(K IpAddr, V i64);
map_as_rust!(K IpAddr, V i32);
map_as_rust!(K IpAddr, V i16);
map_as_rust!(K IpAddr, V i8);
map_as_rust!(K IpAddr, V f64);
map_as_rust!(K IpAddr, V f32);
map_as_rust!(K IpAddr, V IpAddr);
map_as_rust!(K IpAddr, V Uuid);
map_as_rust!(K IpAddr, V List);
map_as_rust!(K IpAddr, V Map);
map_as_rust!(K IpAddr, V UDT);

map_as_rust!(K Uuid, V Vec V < V u8 V >);
map_as_rust!(K Uuid, V String);
map_as_rust!(K Uuid, V bool);
map_as_rust!(K Uuid, V i64);
map_as_rust!(K Uuid, V i32);
map_as_rust!(K Uuid, V i16);
map_as_rust!(K Uuid, V i8);
map_as_rust!(K Uuid, V f64);
map_as_rust!(K Uuid, V f32);
map_as_rust!(K Uuid, V IpAddr);
map_as_rust!(K Uuid, V Uuid);
map_as_rust!(K Uuid, V List);
map_as_rust!(K Uuid, V Map);
map_as_rust!(K Uuid, V UDT);
