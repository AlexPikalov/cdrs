/// instead of writing functions which resemble
/// ```
/// pub fn query<'a> (&'a mut self,query: String) -> &'a mut Self{
///     self.query = Some(query);
///            self
/// }
/// ```
/// and repeating it for all the attributes; it is extracted out as a macro so that code
/// is more concise see
/// @https://doc.rust-lang.org/book/method-syntax.html
///
///
///
macro_rules! builder_opt_field {
    ($field:ident, $field_type:ty) => {
        pub fn $field(mut self,
                          $field: $field_type) -> Self {
            self.$field = Some($field);
            self
        }
    };
}



macro_rules! list_as_rust {
    ($($into_type:tt)*) => (
        impl AsRust<Vec<$($into_type)*>> for List {
            fn as_rust(&self) -> Result<Vec<$($into_type)*>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let convert = self
                            .map(|bytes| as_rust!(type_option_ref, bytes, $($into_type)*).unwrap());

                        Ok(convert)
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
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
                            let key_type_option = key_type_option.as_ref();
                            let val_type_option = val_type_option.as_ref();
                            let key = as_rust!(key_type_option, key, $($key_type)*)?;
                            let val = as_rust!(val_type_option, val, $($val_type)*)?;
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




macro_rules! into_rust_by_name {
    (Row, $($into_type:tt)*) => (
        impl IntoRustByName<$($into_type)*> for Row {
            fn get_by_name(&self, name: &str) -> Option<Result<$($into_type)*>> {
                self.get_col_spec_by_name(name)
                    .map(|(col_spec, cbytes)| {
                        if cbytes.is_empty() {
                            return Err(column_is_empty_err());
                        }

                        let ref col_type = col_spec.col_type;
                        as_rust!(col_type, cbytes, $($into_type)*)
                    })
            }
        }
    );

    (UDT, $($into_type:tt)*) => (
        impl IntoRustByName<$($into_type)*> for UDT {
            fn get_by_name(&self, name: &str) -> Option<Result<$($into_type)*>> {
                self.data.get(name).map(|v| {
                    let &(ref col_type, ref bytes) = v;

                    if bytes.as_plain().is_empty() {
                        return Err(column_is_empty_err());
                    }

                    let converted = as_rust!(col_type, bytes, $($into_type)*);
                    converted.map_err(|err| err.into())
                })
            }
        }
    );
}




/// Decodes any Cassandra data type into the corresponding Rust type,
/// given the column type as `ColTypeOption` and the value as `CBytes`
/// plus the matching Rust type.
macro_rules! as_rust {
    ($data_type_option:ident, $data_value:ident, Vec<u8>) => (
        match $data_type_option.id {
            ColType::Blob => {
                decode_blob($data_value.as_plain())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into Vec<u8> (valid types: Blob).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, String) => (
        match $data_type_option.id {
            ColType::Custom => {
                decode_custom($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Ascii => {
                decode_ascii($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Varchar => {
                decode_varchar($data_value.as_slice())
                    .map_err(Into::into)
            }
            // TODO: clarify when to use decode_text.
            // it's not mentioned in
            // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
            // ColType::XXX => decode_text($data_value)?
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into String (valid types: Custom, Ascii, Varchar).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, bool) => (
        match $data_type_option.id {
            ColType::Boolean => {
                decode_boolean($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into bool (valid types: Boolean).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i64) => (
        match $data_type_option.id {
            ColType::Bigint => {
                decode_bigint($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Timestamp => {
                decode_timestamp($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Time => {
                decode_time($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Varint => {
                decode_varint($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time, Variant).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i32) => (
        match $data_type_option.id {
            ColType::Int => {
                decode_int($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Date => {
                decode_date($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i32 (valid types: Int, Date).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i16) => (
        match $data_type_option.id {
            ColType::Smallint => {
                decode_smallint($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i16 (valid types: Smallint).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i8) => (
        match $data_type_option.id {
            ColType::Tinyint => {
                decode_tinyint($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i8 (valid types: Tinyint).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, f64) => (
        match $data_type_option.id {
            ColType::Double => {
                decode_double($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into f64 (valid types: Double).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, f32) => (
        match $data_type_option.id {
            ColType::Decimal => {
                decode_decimal($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Float => {
                decode_float($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into f32 (valid types: Decimal, Float).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, IpAddr) => (
        match $data_type_option.id {
            ColType::Inet => {
                decode_inet($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into IpAddr (valid types: Inet).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, Uuid) => (
        match $data_type_option.id {
            ColType::Uuid |
            ColType::Timeuuid => {
                decode_timeuuid($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into Uuid (valid types: Uuid, Timeuuid).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, List) => (
        match $data_type_option.id {
            ColType::List |
            ColType::Set => {
                decode_list($data_value.as_slice())
                    .map(|data| List::new(data, $data_type_option.clone()))
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into List (valid types: List, Set).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, Map) => (
        match $data_type_option.id {
            ColType::Map => {
                decode_map($data_value.as_slice())
                    .map(|data| Map::new(data, $data_type_option.clone()))
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into Map (valid types: Map).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, UDT) => (
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Udt,
                value: Some(ColTypeOptionValue::UdtType(ref list_type_option))
            } => {
                decode_udt($data_value.as_slice(), list_type_option.descriptions.len())
                    .map(|data| UDT::new(data, list_type_option))
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into UDT (valid types: UDT).",
                    $data_type_option.id)))
        }
    );
}
