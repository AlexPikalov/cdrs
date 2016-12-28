use std::net;
use std::collections::HashMap;
use frame::frame_result::{ColTypeOption, CUdt, ColType, ColTypeOptionValue};
use types::{CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;

pub struct UDT {
    data: HashMap<String, (ColTypeOption, CBytes)>
}

impl UDT {
    pub fn new(data: Vec<CBytes>, metadata: CUdt) -> UDT {
        let meta_iter = metadata.descriptions.iter();

        let acc: HashMap<String, (ColTypeOption, CBytes)> = HashMap::with_capacity(metadata.descriptions.len());
        let d = meta_iter
            .zip(data.iter())
            .fold(acc, |mut a, v| {
                let (m, val_b) = v;
                let &(ref name_b, ref val_type) = m;
                let name = name_b.as_plain();
                a.insert(name, (val_type.clone(), val_b.clone()));
                return a;
            });

        return UDT {
            data: d
        };
    }
}

impl IntoRustByName<Vec<u8>> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Vec<u8>> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Blob => decode_blob(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<String> for UDT {
    fn get_by_name(&self, name: &str) -> Option<String> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Custom => decode_custom(bytes.as_plain()).ok(),
                ColType::Ascii => decode_ascii(bytes.as_plain()).ok(),
                ColType::Varchar => decode_varchar(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<bool> for UDT {
    fn get_by_name(&self, name: &str) -> Option<bool> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Boolean => decode_boolean(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<i64> for UDT {
    fn get_by_name(&self, name: &str) -> Option<i64> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Bigint => decode_bigint(bytes.as_plain()).ok(),
                ColType::Timestamp => decode_timestamp(bytes.as_plain()).ok(),
                ColType::Time => decode_time(bytes.as_plain()).ok(),
                ColType::Varint => decode_varint(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<i32> for UDT {
    fn get_by_name(&self, name: &str) -> Option<i32> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Int => decode_int(bytes.as_plain()).ok(),
                ColType::Date => decode_date(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<i16> for UDT {
    fn get_by_name(&self, name: &str) -> Option<i16> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Smallint => decode_smallint(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<f64> for UDT {
    fn get_by_name(&self, name: &str) -> Option<f64> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Double => decode_double(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<f32> for UDT {
    fn get_by_name(&self, name: &str) -> Option<f32> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Decimal => decode_decimal(bytes.as_plain()).ok(),
                ColType::Float => decode_float(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<net::IpAddr> for UDT {
    fn get_by_name(&self, name: &str) -> Option<net::IpAddr> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Inet => decode_inet(bytes.as_plain()).ok(),
                _ => None
            }
        });
    }
}

impl IntoRustByName<List> for UDT {
    fn get_by_name(&self, name: &str) -> Option<List> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::List => {
                    let list_bytes = decode_list(bytes.as_plain()).unwrap();
                    Some(List::new(list_bytes, col_type.clone().clone()))
                },
                ColType::Set => {
                    let list_bytes = decode_set(bytes.as_plain()).unwrap();
                    Some(List::new(list_bytes, col_type.clone().clone()))
                },
                _ => None
            }
        });
    }
}

impl IntoRustByName<Map> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Map> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            let list_bytes = decode_map(bytes.as_plain()).unwrap();
            return match col_type.id {
                ColType::Map => Some(Map::new(list_bytes, col_type.clone().clone())),
                _ => None
            }
        });
    }
}

impl IntoRustByName<UDT> for UDT {
    fn get_by_name(&self, name: &str) -> Option<UDT> {
        return self.data.get(name).and_then(|v| {
            let &(ref col_type, ref bytes) = v;
            let list_bytes: Vec<CBytes> = decode_udt(bytes.as_plain()).unwrap();

            if col_type.value.is_none() {
                return None;
            }

            let col_type_value = match col_type.value.as_ref() {
                Some(&ColTypeOptionValue::UdtType(ref ctv)) => ctv,
                _ => return None
            };;

            return match col_type.id {
                ColType::Udt => Some(UDT::new(list_bytes, col_type_value.clone())),
                _ => None
            }
        });
    }
}
