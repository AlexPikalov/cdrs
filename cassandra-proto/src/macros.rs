#[macro_export]
macro_rules! query_values {
    ($($value:expr),*) => {
        {
            use cdrs::types::value::Value;
            use cdrs::query::QueryValues;
            let mut values: Vec<Value> = Vec::new();
            $(
                values.push($value.into());
            )*
            QueryValues::SimpleValues(values)
        }
    };
    ($($name:expr => $value:expr),*) => {
        {
            use cdrs::types::value::Value;
            use cdrs::query::QueryValues;
            use std::collections::HashMap;
            let mut values: HashMap<String, Value> = HashMap::new();
            $(
                values.insert($name.to_string(), $value.into());
            )*
            QueryValues::NamedValues(values)
        }
    };
}

#[macro_export]
macro_rules! builder_opt_field {
    ($field:ident, $field_type:ty) => {
        pub fn $field(mut self,
                          $field: $field_type) -> Self {
            self.$field = Some($field);
            self
        }
    };
}

#[macro_export]
macro_rules! list_as_rust {
    ($($into_type:tt)+) => (
        impl AsRustType<Vec<$($into_type)+>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<$($into_type)+>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, $($into_type)+)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
}

#[macro_export]
macro_rules! map_as_rust {
    ({ $($key_type:tt)+ }, { $($val_type:tt)+ }) => (
        impl AsRustType<HashMap<$($key_type)+, $($val_type)+>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, $($val_type)+>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CMap((ref key_type_option, ref val_type_option))) => {
                        let mut map = HashMap::with_capacity(self.data.len());

                        for &(ref key, ref val) in self.data.iter() {
                            let key_type_option = key_type_option.as_ref();
                            let val_type_option = val_type_option.as_ref();
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, $($val_type)+)?;
                            if val.is_some() && key.is_some() {
                                map.insert(key.unwrap(), val.unwrap());
                            }
                        }

                        Ok(Some(map))
                    }
                    _ => unreachable!()
                }
            }
        }
    );
}

#[macro_export]
macro_rules! into_rust_by_name {
    (Row, $($into_type:tt)+) => (
        impl IntoRustByName<$($into_type)+> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<$($into_type)+>> {
                self.get_col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let ref col_type = col_spec.col_type;
                        as_rust_type!(col_type, cbytes, $($into_type)+)
                    })
            }
        }
    );
    (UDT, $($into_type:tt)+) => (
        impl IntoRustByName<$($into_type)+> for UDT {
            fn get_by_name(&self, name: &str) -> Result<Option<$($into_type)+>> {
                self.data.get(name)
                .ok_or(column_is_empty_err(name))
                .and_then(|v| {
                    let &(ref col_type, ref bytes) = v;
                    let converted = as_rust_type!(col_type, bytes, $($into_type)+);
                    converted.map_err(|err| err.into())
                })
            }
        }
    );
}

#[macro_export]
macro_rules! into_rust_by_index {
    (Tuple, $($into_type:tt)+) => (
        impl IntoRustByIndex<$($into_type)+> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<$($into_type)+>> {
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, $($into_type)+);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Row, $($into_type:tt)+) => (
        impl IntoRustByIndex<$($into_type)+> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<$($into_type)+>> {
                self.get_col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let ref col_type = col_spec.col_type;
                        as_rust_type!(col_type, cbytes, $($into_type)+)
                    })
            }
        }
    );
}

#[macro_export]
macro_rules! as_res_opt {
    ($data_value:ident, $deserialize:expr) => {
        match $data_value.as_plain() {
            Some(ref bytes) => ($deserialize)(bytes).map(|v| Some(v)).map_err(Into::into),
            None => Ok(None),
        }
    };
}

/// Decodes any Cassandra data type into the corresponding Rust type,
/// given the column type as `ColTypeOption` and the value as `CBytes`
/// plus the matching Rust type.
#[macro_export]
macro_rules! as_rust_type {
    ($data_type_option:ident, $data_value:ident, Blob) => {
        match $data_type_option.id {
            ColType::Blob => as_res_opt!($data_value, decode_blob),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Vec<u8> (valid types: Blob).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, String) => {
        match $data_type_option.id {
            ColType::Custom => as_res_opt!($data_value, decode_custom),
            ColType::Ascii => as_res_opt!($data_value, decode_ascii),
            ColType::Varchar => as_res_opt!($data_value, decode_varchar),
            // TODO: clarify when to use decode_text.
            // it's not mentioned in
            // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
            // ColType::XXX => decode_text($data_value)?
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into String (valid types: Custom, Ascii, Varchar).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, bool) => {
        match $data_type_option.id {
            ColType::Boolean => as_res_opt!($data_value, decode_boolean),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into bool (valid types: Boolean).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i64) => {
        match $data_type_option.id {
            ColType::Bigint => as_res_opt!($data_value, decode_bigint),
            ColType::Timestamp => as_res_opt!($data_value, decode_timestamp),
            ColType::Time => as_res_opt!($data_value, decode_time),
            ColType::Varint => as_res_opt!($data_value, decode_varint),
            ColType::Counter => as_res_opt!($data_value, decode_bigint),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time, Variant,\
                 Counter).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i32) => {
        match $data_type_option.id {
            ColType::Int => as_res_opt!($data_value, decode_int),
            ColType::Date => as_res_opt!($data_value, decode_date),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i32 (valid types: Int, Date).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i16) => {
        match $data_type_option.id {
            ColType::Smallint => as_res_opt!($data_value, decode_smallint),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i16 (valid types: Smallint).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i8) => {
        match $data_type_option.id {
            ColType::Tinyint => as_res_opt!($data_value, decode_tinyint),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i8 (valid types: Tinyint).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, f64) => {
        match $data_type_option.id {
            ColType::Double => as_res_opt!($data_value, decode_double),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into f64 (valid types: Double).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, f32) => {
        match $data_type_option.id {
            ColType::Float => as_res_opt!($data_value, decode_float),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into f32 (valid types: Float).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, IpAddr) => {
        match $data_type_option.id {
            ColType::Inet => as_res_opt!($data_value, decode_inet),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into IpAddr (valid types: Inet).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Uuid) => {
        match $data_type_option.id {
            ColType::Uuid | ColType::Timeuuid => as_res_opt!($data_value, decode_timeuuid),
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Uuid (valid types: Uuid, Timeuuid).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, List) => {
        match $data_type_option.id {
            ColType::List | ColType::Set => match $data_value.as_slice() {
                Some(ref bytes) => decode_list(bytes)
                    .map(|data| Some(List::new(data, $data_type_option.clone())))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into List (valid types: List, Set).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Map) => {
        match $data_type_option.id {
            ColType::Map => match $data_value.as_slice() {
                Some(ref bytes) => decode_map(bytes)
                    .map(|data| Some(Map::new(data, $data_type_option.clone())))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Map (valid types: Map).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, UDT) => {
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Udt,
                value: Some(ColTypeOptionValue::UdtType(ref list_type_option)),
            } => match $data_value.as_slice() {
                Some(ref bytes) => decode_udt(bytes, list_type_option.descriptions.len())
                    .map(|data| Some(UDT::new(data, list_type_option)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into UDT (valid types: UDT).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Tuple) => {
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Tuple,
                value: Some(ColTypeOptionValue::TupleType(ref list_type_option)),
            } => match $data_value.as_slice() {
                Some(ref bytes) => decode_tuple(bytes, list_type_option.types.len())
                    .map(|data| Some(Tuple::new(data, list_type_option)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Tuple (valid types: tuple).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Timespec) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| Some(Timespec::new(ts / 1_000, (ts % 1_000 * 1_000_000) as i32)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Timespec (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Decimal) => {
        match $data_type_option.id {
            ColType::Decimal => match $data_value.as_slice() {
                Some(ref bytes) => decode_decimal(bytes).map(|d| Some(d)).map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Decimal (valid types: Decimal).",
                $data_type_option.id
            ))),
        }
    };
}
