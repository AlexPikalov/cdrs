use std::net;
use std::collections::HashMap;
use uuid::Uuid;

use frame::frame_result::{ColTypeOption, CUdt, ColType, ColTypeOptionValue};
use types::{CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use error::Result;

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
                return a;
            });

        return UDT { data: d };
    }
}

impl IntoRustByName<Vec<u8>> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<Vec<u8>>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::Blob => decode_blob(bytes.as_plain()).map_err(|err| err.into()),
                _ => unreachable!(),
            };
        });
    }
}

impl IntoRustByName<String> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<String>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Custom => decode_custom(bytes.as_slice()),
                ColType::Ascii => decode_ascii(bytes.as_slice()),
                ColType::Varchar => decode_varchar(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<bool> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<bool>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Boolean => decode_boolean(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<i64> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<i64>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Bigint => decode_bigint(bytes.as_slice()),
                ColType::Timestamp => decode_timestamp(bytes.as_slice()),
                ColType::Time => decode_time(bytes.as_slice()),
                ColType::Varint => decode_varint(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<i32> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<i32>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Int => decode_int(bytes.as_slice()),
                ColType::Date => decode_date(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<i16> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<i16>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Smallint => decode_smallint(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<f64> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<f64>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Double => decode_double(bytes.as_plain()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<f32> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<f32>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Decimal => decode_decimal(bytes.as_slice()),
                ColType::Float => decode_float(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<net::IpAddr> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<net::IpAddr>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Inet => decode_inet(bytes.as_slice()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<Uuid> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<Uuid>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let converted = match col_type.id {
                ColType::Uuid => decode_timeuuid(bytes.as_plain()),
                ColType::Timeuuid => decode_timeuuid(bytes.as_plain()),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<List> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<List>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            return match col_type.id {
                ColType::List => {
                    let list_bytes = decode_list(bytes.as_plain()).unwrap();
                    Ok(List::new(list_bytes, col_type.clone().clone()))
                }
                ColType::Set => {
                    let list_bytes = decode_set(bytes.as_plain()).unwrap();
                    Ok(List::new(list_bytes, col_type.clone().clone()))
                }
                _ => unreachable!(),
            };
        });
    }
}

impl IntoRustByName<Map> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<Map>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let list_bytes = decode_map(bytes.as_plain()).unwrap();
            return match col_type.id {
                ColType::Map => Ok(Map::new(list_bytes, col_type.clone().clone())),
                _ => unreachable!(),
            };
        });
    }
}

impl IntoRustByName<UDT> for UDT {
    fn get_by_name(&self, name: &str) -> Option<Result<UDT>> {
        return self.data.get(name).map(|v| {
            let &(ref col_type, ref bytes) = v;
            let list_bytes: Vec<CBytes> = try!(decode_udt(bytes.as_plain()));

            if col_type.value.is_none() {
                unreachable!();
            }

            let col_type_value = match col_type.value.as_ref() {
                Some(&ColTypeOptionValue::UdtType(ref ctv)) => ctv,
                _ => unreachable!(),
            };

            return match col_type.id {
                ColType::Udt => Ok(UDT::new(list_bytes, col_type_value)),
                _ => unreachable!(),
            };
        });
    }
}
