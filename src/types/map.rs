use std::collections::HashMap;
use std::net;
use types::{AsRust, CBytes};
use frame::frame_result::{ColTypeOption, ColTypeOptionValue, ColType};
use types::data_serialization_types::*;
use types::list::List;
use types::udt::UDT;

pub struct Map {
    metadata: ColTypeOption,
    data: HashMap<String, CBytes>
}

impl Map {
    /// Creates new `Map` basing on provided data and key and value types.
    // TODO: Need to return Result<Map>. If key has not string-like type
    // return Err otherwise return Ok.
    pub fn new(data: Vec<(CBytes, CBytes)>, meta: ColTypeOption) -> Map {
        let accumulator: HashMap<String, CBytes> = HashMap::new();
        let map_option_type = meta.value.clone();

        if map_option_type.is_none() {
            unimplemented!();
        }

        // check that key could be converted into String
        let serializer = if let ColTypeOptionValue::CMap((key_type, _)) = map_option_type.unwrap() {
            match key_type.id {
                ColType::Custom => decode_custom,
                ColType::Ascii => decode_ascii,
                ColType::Varchar => decode_varchar,
                _ => unimplemented!()
            }
        } else {
            unimplemented!();
        };

        let map: HashMap<String, CBytes> = data
            .iter()
            .fold(accumulator, |mut acc, kv| {
                let (key_b, value_b) = kv.clone();
                let key: String = serializer(key_b.as_plain()).unwrap();

                acc.insert(key, value_b);

                return acc;
            });

        return Map {
            metadata: meta,
            data: map
        };
    }
}

// into hash map which values are blobs
impl AsRust<HashMap<String, Vec<u8>>> for Map {
    /// Converts `Map` into `HashMap<String, Vec<u8>>` for blob values.
    fn as_rust(&self) -> Option<HashMap<String, Vec<u8>>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, Vec<u8>> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Blob => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_blob(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, String>> for Map {
    /// Converts `Map` into `HashMap<String, String>` for string-like values.
    fn as_rust(&self) -> Option<HashMap<String, String>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, String> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Custom => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_custom(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Ascii => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_ascii(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Varchar => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_varchar(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, bool>> for Map {
    /// Converts `Map` into `HashMap<String, bool>` for boolean values.
    fn as_rust(&self) -> Option<HashMap<String, bool>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, bool> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Boolean => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_boolean(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, i64>> for Map {
    /// Converts `Map` into `HashMap<String, i64>` for numerical values.
    fn as_rust(&self) -> Option<HashMap<String, i64>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, i64> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Bigint => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_bigint(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Timestamp => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_timestamp(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Time => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_time(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Varint => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_varint(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, i32>> for Map {
    /// Converts `Map` into `HashMap<String, i32>` for numerical values.
    fn as_rust(&self) -> Option<HashMap<String, i32>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, i32> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Int => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_int(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Date => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_date(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, i16>> for Map {
    /// Converts `Map` into `HashMap<String, i16>` for numerical values.
    fn as_rust(&self) -> Option<HashMap<String, i16>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, i16> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Smallint => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_smallint(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, f64>> for Map {
    /// Converts `Map` into `HashMap<String, f64>` for numerical values.
    fn as_rust(&self) -> Option<HashMap<String, f64>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, f64> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Double => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_double(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, f32>> for Map {
    /// Converts `Map` into `HashMap<String, f32>` for numerical values.
    fn as_rust(&self) -> Option<HashMap<String, f32>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, f32> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Decimal => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_decimal(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    ColType::Float => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_float(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, net::IpAddr>> for Map {
    /// Converts `Map` into `HashMap<String, net::IpAddr>` for IP address values.
    fn as_rust(&self) -> Option<HashMap<String, net::IpAddr>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, net::IpAddr> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Inet => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                acc.insert(k.clone(), decode_inet(vb.as_plain()).unwrap());
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, List>> for Map {
    /// Converts `Map` into `HashMap<String, List>` for List address values.
    fn as_rust(&self) -> Option<HashMap<String, List>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, List> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::List => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = List::new(
                                    decode_list(vb.as_plain()).unwrap(),
                                    value_type_option.as_ref().clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            })
                    ),
                    ColType::Set => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = List::new(
                                    decode_list(vb.as_plain()).unwrap(),
                                    value_type_option.as_ref().clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, Map>> for Map {
    /// Converts `Map` into `HashMap<String, Map>` for Map address values.
    fn as_rust(&self) -> Option<HashMap<String, Map>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, Map> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                match value_type_option.id {
                    ColType::Map => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = Map::new(
                                    decode_map(vb.as_plain()).unwrap(),
                                    value_type_option.as_ref().clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

impl AsRust<HashMap<String, UDT>> for Map {
    /// Converts `Map` into `HashMap<String, Map>` for Map address values.
    fn as_rust(&self) -> Option<HashMap<String, UDT>> {
        if self.metadata.value.is_none() {
            return None;
        }

        let map: HashMap<String, UDT> = HashMap::new();

        match self.metadata.value.clone().unwrap() {
            ColTypeOptionValue::CMap((_, value_type_option)) => {
                let list_type_option = match value_type_option.value {
                    Some(ColTypeOptionValue::UdtType(ref t)) => t,
                    _ => return None
                };

                match value_type_option.id {
                    ColType::Udt => Some(
                        self.data
                            .iter()
                            .fold(map, |mut acc, (k, vb)| {
                                let list = UDT::new(
                                    decode_udt(vb.as_plain()).unwrap(),
                                    list_type_option.clone());
                                acc.insert(k.clone(), list);
                                return acc;
                            })
                    ),
                    _ => None
                }
            },
            _ => None
        }
    }
}

// TODO: uuid
