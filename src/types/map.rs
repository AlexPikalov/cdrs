use std::collections::HashMap;
use std::net;
use uuid::Uuid;

use types::{AsRust, CBytes};
use frame::frame_result::{ColTypeOption, ColTypeOptionValue, ColType};
use types::data_serialization_types::*;
use types::list::List;
use types::udt::UDT;
use error::Result;

#[derive(Debug)]
pub struct Map {
    metadata: ColTypeOption,
    data: HashMap<String, CBytes>,
}

impl Map {
    /// Creates new `Map` basing on provided data and key and value types.
    // TODO: Need to return Result<Map>. If key has not string-like type
    // return Err otherwise return Ok.
    pub fn new(data: Vec<(CBytes, CBytes)>, meta: ColTypeOption) -> Map {
        let accumulator: HashMap<String, CBytes> = HashMap::new();

        // check that key could be converted into String
        let serializer = if let Some(ColTypeOptionValue::CMap((ref key_type, _))) = meta.value {
            match key_type.id {
                ColType::Custom => decode_custom,
                ColType::Ascii => decode_ascii,
                ColType::Varchar => decode_varchar,
                // unreachable ??
                // do we need this arm? is it reachable due to the protocol?
                _ => unimplemented!(),
            }
        } else {
            // do we need this arm? is it reachable due to the protocol?
            unreachable!();
        };

        let map: HashMap<String, CBytes> = data.iter()
            .fold(accumulator, |mut acc, kv| {
                let (key_b, value_b) = kv.clone();
                let key: String = serializer(key_b.as_slice()).unwrap();

                acc.insert(key, value_b);

                return acc;
            });

        return Map {
            metadata: meta,
            data: map,
        };
    }
}

// into hash map which values are blobs
impl AsRust<HashMap<String, Vec<u8>>> for Map {
    /// Converts `Map` into `HashMap<String, Vec<u8>>` for blob values.
    fn as_rust(&self) -> Result<HashMap<String, Vec<u8>>> {
        let map: HashMap<String, Vec<u8>> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Blob => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_blob(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, String>> for Map {
    /// Converts `Map` into `HashMap<String, String>` for string-like values.
    fn as_rust(&self) -> Result<HashMap<String, String>> {
        let map: HashMap<String, String> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Custom => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_custom(vb.as_slice()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Ascii => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_ascii(vb.as_slice()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Varchar => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_varchar(vb.as_slice()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, bool>> for Map {
    /// Converts `Map` into `HashMap<String, bool>` for boolean values.
    fn as_rust(&self) -> Result<HashMap<String, bool>> {
        let map: HashMap<String, bool> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Boolean => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_boolean(vb.as_slice()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, i64>> for Map {
    /// Converts `Map` into `HashMap<String, i64>` for numerical values.
    fn as_rust(&self) -> Result<HashMap<String, i64>> {
        let map: HashMap<String, i64> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Bigint => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_bigint(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Timestamp => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_timestamp(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Time => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_time(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Varint => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_varint(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, i32>> for Map {
    /// Converts `Map` into `HashMap<String, i32>` for numerical values.
    fn as_rust(&self) -> Result<HashMap<String, i32>> {
        let map: HashMap<String, i32> = HashMap::new();

        // FIXME: implement via maps
        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Int => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_int(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Date => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_date(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, i16>> for Map {
    /// Converts `Map` into `HashMap<String, i16>` for numerical values.
    fn as_rust(&self) -> Result<HashMap<String, i16>> {
        let map: HashMap<String, i16> = HashMap::new();

        // FIXME
        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Smallint => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_smallint(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, f64>> for Map {
    /// Converts `Map` into `HashMap<String, f64>` for numerical values.
    fn as_rust(&self) -> Result<HashMap<String, f64>> {
        let map: HashMap<String, f64> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Double => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_double(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, f32>> for Map {
    /// Converts `Map` into `HashMap<String, f32>` for numerical values.
    fn as_rust(&self) -> Result<HashMap<String, f32>> {
        let map: HashMap<String, f32> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Decimal => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_decimal(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Float => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_float(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, net::IpAddr>> for Map {
    /// Converts `Map` into `HashMap<String, net::IpAddr>` for IP address values.
    fn as_rust(&self) -> Result<HashMap<String, net::IpAddr>> {
        let map: HashMap<String, net::IpAddr> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Inet => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_inet(vb.as_slice()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, Uuid>> for Map {
    /// Converts `Map` into `HashMap<String, Uuid>` for IP address values.
    fn as_rust(&self) -> Result<HashMap<String, Uuid>> {
        let map: HashMap<String, Uuid> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Uuid => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_timeuuid(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    ColType::Timeuuid => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_timeuuid(vb.as_plain()).unwrap());
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, List>> for Map {
    /// Converts `Map` into `HashMap<String, List>` for List address values.
    fn as_rust(&self) -> Result<HashMap<String, List>> {
        let map: HashMap<String, List> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::List => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = List::new(decode_list(vb.as_plain()).unwrap(),
                                                     value_type_option.as_ref().clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            }))
                    }
                    ColType::Set => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = List::new(decode_list(vb.as_plain()).unwrap(),
                                                     value_type_option.as_ref().clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, Map>> for Map {
    /// Converts `Map` into `HashMap<String, Map>` for Map address values.
    fn as_rust(&self) -> Result<HashMap<String, Map>> {
        let map: HashMap<String, Map> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                match value_type_option.id {
                    ColType::Map => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = Map::new(decode_map(vb.as_plain()).unwrap(),
                                                    value_type_option.as_ref().clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl AsRust<HashMap<String, UDT>> for Map {
    /// Converts `Map` into `HashMap<String, Map>` for Map address values.
    fn as_rust(&self) -> Result<HashMap<String, UDT>> {
        let map: HashMap<String, UDT> = HashMap::new();

        match self.metadata.value {
            Some(ColTypeOptionValue::CMap((_, ref value_type_option))) => {
                let list_type_option = match value_type_option.value {
                    Some(ColTypeOptionValue::UdtType(ref t)) => t,
                    _ => unreachable!(),
                };

                match value_type_option.id {
                    ColType::Udt => {
                        Ok(self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = UDT::new(decode_udt(vb.as_plain()).unwrap(),
                                                    list_type_option);
                                acc.insert(k.clone(), list);
                                return acc;
                            }))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

// TODO: uuid
