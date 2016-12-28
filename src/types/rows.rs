use std::net;

use frame::frame_result::{RowsMetadata, ColType, ColSpec, BodyResResultRows};
use types::{CBytes, IntoRustByName};
use types::data_serialization_types::*;
use types::list::List;
use types::map::Map;

pub struct Row {
    metadata: RowsMetadata,
    row_content: Vec<CBytes>
}

impl Row {
    pub fn from_frame_body(body: BodyResResultRows) -> Vec<Row> {
        return body.rows_content
            .iter()
            .map(|row| Row {
                metadata: body.metadata.clone(),
                row_content: row.clone()
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
    fn get_by_name(&self, name: &str) -> Option<Vec<u8>> {
        // TODO: create try! for Option and replace following code. Same for other IntoRust impls.
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Blob => decode_blob(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<String> for Row {
    fn get_by_name(&self, name: &str) -> Option<String> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Custom => decode_custom(bytes).ok(),
            &ColType::Ascii => decode_ascii(bytes).ok(),
            &ColType::Varchar => decode_varchar(bytes).ok(),
            // TODO: clarify when to use decode_text.
            // it's not mentioned in https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
            // &ColType::XXX => decode_text(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<bool> for Row {
    fn get_by_name(&self, name: &str) -> Option<bool> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Boolean => decode_boolean(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<i64> for Row {
    fn get_by_name(&self, name: &str) -> Option<i64> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Bigint => decode_bigint(bytes).ok(),
            &ColType::Timestamp => decode_timestamp(bytes).ok(),
            &ColType::Time => decode_time(bytes).ok(),
            &ColType::Varint => decode_varint(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<i32> for Row {
    fn get_by_name(&self, name: &str) -> Option<i32> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Int => decode_int(bytes).ok(),
            &ColType::Date => decode_date(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<i16> for Row {
    fn get_by_name(&self, name: &str) -> Option<i16> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Smallint => decode_smallint(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<f64> for Row {
    fn get_by_name(&self, name: &str) -> Option<f64> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Double => decode_double(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<f32> for Row {
    fn get_by_name(&self, name: &str) -> Option<f32> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Decimal => decode_decimal(bytes).ok(),
            &ColType::Float => decode_float(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<net::IpAddr> for Row {
    fn get_by_name(&self, name: &str) -> Option<net::IpAddr> {
        let col = self.get_col_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type {
            &ColType::Inet => decode_inet(bytes).ok(),
            _ => None
        }
    }
}

impl IntoRustByName<List> for Row {
    fn get_by_name(&self, name: &str) -> Option<List> {
        let col = self.get_col_spec_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type.col_type.id {
            ColType::List => Some(List::new(decode_list(bytes).unwrap(), cassandra_type.col_type.clone())),
            ColType::Set => Some(List::new(decode_set(bytes).unwrap(), cassandra_type.col_type.clone())),
            _ => None
        }
    }
}

impl IntoRustByName<Map> for Row {
    fn get_by_name(&self, name: &str) -> Option<Map> {
        let col = self.get_col_spec_by_name(name);
        if col.is_none() {
            return None;
        }
        let (cassandra_type, cbytes) = col.unwrap();
        let bytes = cbytes.as_plain().clone();

        return match cassandra_type.col_type.id {
            ColType::Map => Some(Map::new(decode_map(bytes).unwrap(), cassandra_type.col_type.clone())),
            _ => None
        }
    }
}

//TODO: add udt
