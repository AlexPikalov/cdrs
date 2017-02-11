use std::net;
use uuid::Uuid;

use frame::frame_result::{RowsMetadata, ColType, ColSpec, BodyResResultRows, ColTypeOptionValue};
use types::{CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;
use types::udt::UDT;
use error::Result;
use std::io;

#[derive(Debug)]
pub struct Row {
    metadata: RowsMetadata,
    row_content: Vec<CBytes>,
}

impl Row {
    pub fn from_frame_body(body: BodyResResultRows) -> Vec<Row> {
        return body.rows_content
            .iter()
            .map(|row| {
                Row {
                    metadata: body.metadata.clone(),
                    row_content: row.clone(),
                }
            })
            .collect();
    }

    fn get_col_by_name(&self, name: &str) -> Option<(&ColType, &CBytes)> {
        let i_opt = self.metadata.col_specs.iter().position(|spec| spec.name.as_str() == name);
        if !i_opt.is_some() {
            return None;
        }
        let i = i_opt.unwrap();
        let ref data: CBytes = self.row_content[i];
        let ref cassandra_type: ColType = self.metadata.col_specs[i].col_type.id;
        return Some((cassandra_type, data));
    }

    fn get_col_spec_by_name(&self, name: &str) -> Option<(&ColSpec, &CBytes)> {
        let i_opt = self.metadata.col_specs.iter().position(|spec| spec.name.as_str() == name);
        if !i_opt.is_some() {
            return None;
        }
        let i = i_opt.unwrap();
        let ref data: CBytes = self.row_content[i];
        let ref cassandra_type: ColSpec = self.metadata.col_specs[i];
        return Some((cassandra_type, data));
    }
}

impl IntoRustByName<Vec<u8>> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<Vec<u8>>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();
            let converted = match cassandra_type {
                &ColType::Blob => decode_blob(bytes),
                _ => unreachable!(),
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<String> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<String>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Custom => decode_custom(bytes),
                &ColType::Ascii => decode_ascii(bytes),
                &ColType::Varchar => decode_varchar(bytes),
                // TODO: clarify when to use decode_text.
                // it's not mentioned in
                // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
                // &ColType::XXX => decode_text(bytes).ok(),
                _ => unreachable!(),
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<bool> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<bool>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Boolean => decode_boolean(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (bool ) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<i64> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<i64>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();
            let converted = match cassandra_type {
                &ColType::Int => decode_bigint(bytes),
                &ColType::Bigint => decode_bigint(bytes),
                &ColType::Timestamp => decode_timestamp(bytes),
                &ColType::Time => decode_time(bytes),
                &ColType::Varint => decode_varint(bytes),
                &ColType::Float => decode_varint(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (Int,Bigint,Timestamp,Time,Varint,Float ) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<i32> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<i32>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Int => decode_int(bytes),
                &ColType::Date => decode_date(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (Int,date ) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<i16> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<i16>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Smallint => decode_smallint(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (Smallint) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };
            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<f64> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<f64>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Double => decode_double(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (Double) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<f32> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<f32>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Decimal => decode_decimal(bytes),
                &ColType::Float => decode_float(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (Float,Decimal) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<net::IpAddr> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<net::IpAddr>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Inet => decode_inet(bytes),
                _ => {
                    let io_err =
                        io::Error::new(io::ErrorKind::NotFound,
                                       format!("Unsupported type of converter. {:?} got, but
                    (Inet) is only supported.",
                                               cassandra_type));
                    Err(io_err)
                }
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<Uuid> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<Uuid>> {
        return self.get_col_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            let converted = match cassandra_type {
                &ColType::Uuid => decode_timeuuid(bytes),
                &ColType::Timeuuid => decode_timeuuid(bytes),
                _ => unreachable!(),
            };

            return converted.map_err(|err| err.into());
        });
    }
}

impl IntoRustByName<List> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<List>> {
        return self.get_col_spec_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            return match cassandra_type.col_type.id {
                // in fact, both decode_list and decode_set return Ok
                ColType::List => {
                    Ok(List::new(decode_list(bytes).unwrap(), cassandra_type.col_type.clone()))
                }
                ColType::Set => {
                    Ok(List::new(decode_set(bytes).unwrap(), cassandra_type.col_type.clone()))
                }
                _ => unreachable!(),
            };
        });
    }
}

impl IntoRustByName<Map> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<Map>> {
        return self.get_col_spec_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain().clone();

            return match cassandra_type.col_type.id {
                // in fact, both decode_map and decode_set return Ok
                ColType::Map => {
                    Ok(Map::new(decode_map(bytes).unwrap(), cassandra_type.col_type.clone()))
                }
                _ => unreachable!(),
            };
        });
    }
}

impl IntoRustByName<UDT> for Row {
    fn get_by_name(&self, name: &str) -> Option<Result<UDT>> {
        return self.get_col_spec_by_name(name).map(|(cassandra_type, cbytes)| {
            let bytes = cbytes.as_plain();
            let cudt = match cassandra_type.col_type.value {
                Some(ColTypeOptionValue::UdtType(ref t)) => t,
                _ => unreachable!(),
            };

            return match cassandra_type.col_type.id {
                ColType::Map => Ok(UDT::new(decode_udt(bytes).unwrap(), cudt)),
                _ => unreachable!(),
            };
        });
    }
}

// TODO: add uuid
