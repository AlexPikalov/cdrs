use std::io::Cursor;

use frame::{FromBytes, FromCursor, IntoBytes};
use error;
use types::*;
use types::rows::Row;
use frame::events::SchemaChange;


/// `ResultKind` is enum which represents types of result.
#[derive(Debug)]
pub enum ResultKind {
    /// Void result.
    Void,
    /// Rows result.
    Rows,
    /// Set keyspace result.
    SetKeyspace,
    /// Prepeared result.
    Prepared,
    /// Schema change result.
    SchemaChange,
}

impl IntoBytes for ResultKind {
    fn into_cbytes(&self) -> Vec<u8> {
        match *self {
            ResultKind::Void => to_int(0x0001),
            ResultKind::Rows => to_int(0x0002),
            ResultKind::SetKeyspace => to_int(0x0003),
            ResultKind::Prepared => to_int(0x0004),
            ResultKind::SchemaChange => to_int(0x0005),
        }
    }
}

impl FromBytes for ResultKind {
    fn from_bytes(bytes: &[u8]) -> error::Result<ResultKind> {
        try_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(|r| match r {
                          0x0001 => Ok(ResultKind::Void),
                          0x0002 => Ok(ResultKind::Rows),
                          0x0003 => Ok(ResultKind::SetKeyspace),
                          0x0004 => Ok(ResultKind::Prepared),
                          0x0005 => Ok(ResultKind::SchemaChange),
                          _ => Err("Unexpected result kind".into()),
                      })
    }
}

impl FromCursor for ResultKind {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<ResultKind> {
        cursor_next_value(&mut cursor, INT_LEN as u64)
            .and_then(|bytes| ResultKind::from_bytes(bytes.as_slice()))
    }
}

/// `ResponseBody` is a generalized enum that represents all types of responses. Each of enum
/// option wraps related body type.
#[derive(Debug)]
pub enum ResResultBody {
    /// Void response body. It's an empty stuct.
    Void(BodyResResultVoid),
    /// Rows response body. It represents a body of response which contains rows.
    Rows(BodyResResultRows),
    /// Set keyspace body. It represents a body of set_keyspace query and usually contains
    /// a name of just set namespace.
    SetKeyspace(BodyResResultSetKeyspace),
    /// Prepared response body.
    Prepared(BodyResResultPrepared),
    /// Schema change body
    SchemaChange(SchemaChange),
}

impl ResResultBody {
    /// It retrieves`ResResultBody` from `io::Cursor`
    /// having knowledge about expected kind of result.
    fn parse_body_from_cursor(mut cursor: &mut Cursor<&[u8]>,
                              result_kind: ResultKind)
                              -> error::Result<ResResultBody> {
        Ok(match result_kind {
               ResultKind::Void => {
                   ResResultBody::Void(BodyResResultVoid::from_cursor(&mut cursor)?)
               }
               ResultKind::Rows => {
                   ResResultBody::Rows(BodyResResultRows::from_cursor(&mut cursor)?)
               }
               ResultKind::SetKeyspace => {
                   ResResultBody::SetKeyspace(BodyResResultSetKeyspace::from_cursor(&mut cursor)?)
               }
               ResultKind::Prepared => {
                   ResResultBody::Prepared(BodyResResultPrepared::from_cursor(&mut cursor)?)
               }
               ResultKind::SchemaChange => {
                   ResResultBody::SchemaChange(SchemaChange::from_cursor(&mut cursor)?)
               }
           })
    }

    /// It converts body into `Vec<Row>` if body's type is `Row` and returns `None` otherwise.
    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            ResResultBody::Rows(rows_body) => Some(Row::from_frame_body(rows_body)),
            _ => None,
        }
    }

    /// It unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// PREPARE query.
    pub fn into_prepared(self) -> Option<BodyResResultPrepared> {
        match self {
            ResResultBody::Prepared(p) => Some(p),
            _ => None,
        }
    }

    /// It unwraps body and returns BodyResResultSetKeyspace which contains an exact result of
    /// use keyspace query.
    pub fn into_set_keyspace(self) -> Option<BodyResResultSetKeyspace> {
        match self {
            ResResultBody::SetKeyspace(p) => Some(p),
            _ => None,
        }
    }
}

impl FromCursor for ResResultBody {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<ResResultBody> {
        let result_kind = ResultKind::from_cursor(&mut cursor)?;

        ResResultBody::parse_body_from_cursor(&mut cursor, result_kind)
    }
}

/// Body of a response of type Void
#[derive(Debug, Default)]
pub struct BodyResResultVoid {}


impl FromBytes for BodyResResultVoid {
    fn from_bytes(_bytes: &[u8]) -> error::Result<BodyResResultVoid> {
        // as it's empty by definition just create BodyResVoid
        let body: BodyResResultVoid = Default::default();
        Ok(body)
    }
}

impl FromCursor for BodyResResultVoid {
    fn from_cursor(mut _cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultVoid> {
        let body: BodyResResultVoid = Default::default();
        Ok(body)
    }
}

/// It represents set keyspace result body. Body contains keyspace name.
#[derive(Debug)]
pub struct BodyResResultSetKeyspace {
    /// It contains name of keyspace that was set.
    pub body: CString,
}

impl BodyResResultSetKeyspace {
    /// Factory function that takes body value and
    /// returns new instance of `BodyResResultSetKeyspace`.
    pub fn new(body: CString) -> BodyResResultSetKeyspace {
        BodyResResultSetKeyspace { body: body }
    }
}

impl FromCursor for BodyResResultSetKeyspace {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultSetKeyspace> {
        CString::from_cursor(&mut cursor).map(BodyResResultSetKeyspace::new)
    }
}


/// Structure that represents result of type
/// [rows](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L533).
#[derive(Debug)]
pub struct BodyResResultRows {
    /// Rows metadata
    pub metadata: RowsMetadata,
    /// Number of rows.
    pub rows_count: CInt,
    /// From spec: it is composed of `rows_count` of rows.
    pub rows_content: Vec<Vec<CBytes>>,
}

impl BodyResResultRows {
    /// It retrieves rows content having knowledge about number of rows and columns.
    fn get_rows_content(mut cursor: &mut Cursor<&[u8]>,
                        rows_count: i32,
                        columns_count: i32)
                        -> Vec<Vec<CBytes>> {
        (0..rows_count)
            .map(|_| {
                     (0..columns_count)
                     // XXX unwrap()
                         .map(|_| CBytes::from_cursor(&mut cursor).unwrap() as CBytes)
                         .collect()
                 })
            .collect()
    }
}

impl FromCursor for BodyResResultRows {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultRows> {
        let metadata = RowsMetadata::from_cursor(&mut cursor)?;
        let rows_count = CInt::from_cursor(&mut cursor)?;
        let rows_content: Vec<Vec<CBytes>> =
            BodyResResultRows::get_rows_content(&mut cursor, rows_count, metadata.columns_count);

        Ok(BodyResResultRows {
               metadata: metadata,
               rows_count: rows_count,
               rows_content: rows_content,
           })
    }
}

/// Rows metadata.
#[derive(Debug, Clone)]
pub struct RowsMetadata {
    /// Flags.
    /// [Read more...]
    /// (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L541)
    pub flags: i32,
    /// Number of columns.
    pub columns_count: i32,
    /// Paging state.
    pub paging_state: Option<CBytes>,
    // In fact by specification Vec should have only two elements representing the
    // (unique) keyspace name and table name the columns belong to
    /// `Option` that may contain global table space.
    pub global_table_space: Option<Vec<CString>>,
    /// List of column specifications.
    pub col_specs: Vec<ColSpec>,
}

impl FromCursor for RowsMetadata {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<RowsMetadata> {
        let flags = CInt::from_cursor(&mut cursor)?;
        let columns_count = CInt::from_cursor(&mut cursor)?;

        let mut paging_state: Option<CBytes> = None;
        if RowsMetadataFlag::has_has_more_pages(flags) {
            paging_state = Some(CBytes::from_cursor(&mut cursor)?)
        }

        let mut global_table_space: Option<Vec<CString>> = None;
        let has_global_table_space = RowsMetadataFlag::has_global_table_space(flags);
        if has_global_table_space {
            let keyspace = CString::from_cursor(&mut cursor)?;
            let tablename = CString::from_cursor(&mut cursor)?;
            global_table_space = Some(vec![keyspace, tablename])
        }

        let col_specs = ColSpec::parse_colspecs(&mut cursor, columns_count, has_global_table_space);

        Ok(RowsMetadata {
               flags: flags,
               columns_count: columns_count,
               paging_state: paging_state,
               global_table_space: global_table_space,
               col_specs: col_specs,
           })
    }
}

const GLOBAL_TABLE_SPACE: i32 = 0x0001;
const HAS_MORE_PAGES: i32 = 0x0002;
const NO_METADATA: i32 = 0x0004;

/// Enum that represent a set of possible row metadata flags that could be set.
pub enum RowsMetadataFlag {
    GlobalTableSpace,
    HasMorePages,
    NoMetadata,
}

impl RowsMetadataFlag {
    /// Shows if provided flag contains GlobalTableSpace rows metadata flag
    pub fn has_global_table_space(flag: i32) -> bool {
        (flag & GLOBAL_TABLE_SPACE) != 0
    }

    /// Sets GlobalTableSpace rows metadata flag
    pub fn set_global_table_space(flag: i32) -> i32 {
        flag | GLOBAL_TABLE_SPACE
    }

    /// Shows if provided flag contains HasMorePages rows metadata flag
    pub fn has_has_more_pages(flag: i32) -> bool {
        (flag & HAS_MORE_PAGES) != 0
    }

    /// Sets HasMorePages rows metadata flag
    pub fn set_has_more_pages(flag: i32) -> i32 {
        flag | HAS_MORE_PAGES
    }

    /// Shows if provided flag contains NoMetadata rows metadata flag
    pub fn has_no_metadata(flag: i32) -> bool {
        (flag & NO_METADATA) != 0
    }

    /// Sets NoMetadata rows metadata flag
    pub fn set_no_metadata(flag: i32) -> i32 {
        flag | NO_METADATA
    }
}

impl IntoBytes for RowsMetadataFlag {
    fn into_cbytes(&self) -> Vec<u8> {
        match *self {
            RowsMetadataFlag::GlobalTableSpace => to_int(GLOBAL_TABLE_SPACE),
            RowsMetadataFlag::HasMorePages => to_int(HAS_MORE_PAGES),
            RowsMetadataFlag::NoMetadata => to_int(NO_METADATA),
        }
    }
}

impl FromBytes for RowsMetadataFlag {
    fn from_bytes(bytes: &[u8]) -> error::Result<RowsMetadataFlag> {
        try_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(|f| match f as i32 {
                          GLOBAL_TABLE_SPACE => Ok(RowsMetadataFlag::GlobalTableSpace),
                          HAS_MORE_PAGES => Ok(RowsMetadataFlag::HasMorePages),
                          NO_METADATA => Ok(RowsMetadataFlag::NoMetadata),
                          _ => Err("Unexpected rows metadata flag".into()),
                      })
    }
}

/// Single column specification.
#[derive(Debug, Clone)]
pub struct ColSpec {
    /// The initial <ksname> is a [string] and is only present
    /// if the Global_tables_spec flag is NOT set
    pub ksname: Option<CString>,
    /// The initial <tablename> is a [string] and is present
    /// if the Global_tables_spec flag is NOT set
    pub tablename: Option<CString>,
    /// Column name
    pub name: CString,
    /// Column type defined in spec in 4.2.5.2
    pub col_type: ColTypeOption,
}

impl ColSpec {
    /// parse_colspecs tables mutable cursor,
    /// number of columns (column_count) and flags that indicates
    /// if Global_tables_spec is specified. It returns column_count of ColSpecs.
    pub fn parse_colspecs(mut cursor: &mut Cursor<&[u8]>,
                          column_count: i32,
                          with_globale_table_spec: bool)
                          -> Vec<ColSpec> {
        (0..column_count)
            .map(|_| {
                let ksname: Option<CString> = if !with_globale_table_spec {
                    Some(CString::from_cursor(&mut cursor).unwrap())
                } else {
                    None
                };

                let tablename = if !with_globale_table_spec {
                    Some(CString::from_cursor(&mut cursor).unwrap())
                } else {
                    None
                };

                // XXX unwrap
                let name = CString::from_cursor(&mut cursor).unwrap();
                let col_type = ColTypeOption::from_cursor(&mut cursor).unwrap();

                ColSpec {
                    ksname: ksname,
                    tablename: tablename,
                    name: name,
                    col_type: col_type,
                }
            })
            .collect()
    }
}

/// Cassandra data types which clould be returned by a server.
#[derive(Debug, Clone)]
pub enum ColType {
    Custom,
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    Date,
    Time,
    Smallint,
    Tinyint,
    List,
    Map,
    Set,
    Udt,
    Tuple,
    Null,
}

impl FromBytes for ColType {
    fn from_bytes(bytes: &[u8]) -> error::Result<ColType> {
        try_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(|b| match b {
                          0x0000 => Ok(ColType::Custom),
                          0x0001 => Ok(ColType::Ascii),
                          0x0002 => Ok(ColType::Bigint),
                          0x0003 => Ok(ColType::Blob),
                          0x0004 => Ok(ColType::Boolean),
                          0x0005 => Ok(ColType::Counter),
                          0x0006 => Ok(ColType::Decimal),
                          0x0007 => Ok(ColType::Double),
                          0x0008 => Ok(ColType::Float),
                          0x0009 => Ok(ColType::Int),
                          0x000B => Ok(ColType::Timestamp),
                          0x000C => Ok(ColType::Uuid),
                          0x000D => Ok(ColType::Varchar),
                          0x000E => Ok(ColType::Varint),
                          0x000F => Ok(ColType::Timeuuid),
                          0x0010 => Ok(ColType::Inet),
                          0x0011 => Ok(ColType::Date),
                          0x0012 => Ok(ColType::Time),
                          0x0013 => Ok(ColType::Smallint),
                          0x0014 => Ok(ColType::Tinyint),
                          0x0020 => Ok(ColType::List),
                          0x0021 => Ok(ColType::Map),
                          0x0022 => Ok(ColType::Set),
                          0x0030 => Ok(ColType::Udt),
                          0x0031 => Ok(ColType::Tuple),
                          _ => Err("Unexpected column type".into()),
                      })
    }
}

impl FromCursor for ColType {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<ColType> {
        cursor_next_value(&mut cursor, SHORT_LEN as u64)
            .and_then(|bytes| ColType::from_bytes(bytes.as_slice()))
            .map_err(Into::into)
    }
}

/// Cassandra option that represent column type.
#[derive(Debug, Clone)]
pub struct ColTypeOption {
    /// Id refers to `ColType`.
    pub id: ColType,
    /// Values depending on column type.
    /// [Read more...]
    /// (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L569)
    pub value: Option<ColTypeOptionValue>,
}

impl FromCursor for ColTypeOption {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<ColTypeOption> {
        let id = ColType::from_cursor(&mut cursor)?;
        let value = match id {
            ColType::Custom => {
                Some(ColTypeOptionValue::CString(CString::from_cursor(&mut cursor)?))
            }
            ColType::Set => {
                let col_type = ColTypeOption::from_cursor(&mut cursor)?;
                Some(ColTypeOptionValue::CSet(Box::new(col_type)))
            }
            ColType::List => {
                let col_type = ColTypeOption::from_cursor(&mut cursor)?;
                Some(ColTypeOptionValue::CList(Box::new(col_type)))
            }
            ColType::Udt => Some(ColTypeOptionValue::UdtType(CUdt::from_cursor(&mut cursor)?)),
            ColType::Tuple => {
                Some(ColTypeOptionValue::TupleType(CTuple::from_cursor(&mut cursor)?))
            }
            ColType::Map => {
                let name_type = ColTypeOption::from_cursor(&mut cursor)?;
                let value_type = ColTypeOption::from_cursor(&mut cursor)?;
                Some(ColTypeOptionValue::CMap((Box::new(name_type), Box::new(value_type))))
            }
            _ => None,
        };

        Ok(ColTypeOption {
               id: id,
               value: value,
           })
    }
}

/// Enum that represents all possible types of `value` of `ColTypeOption`.
#[derive(Debug, Clone)]
pub enum ColTypeOptionValue {
    CString(CString),
    ColType(ColType),
    CSet(Box<ColTypeOption>),
    CList(Box<ColTypeOption>),
    UdtType(CUdt),
    TupleType(CTuple),
    CMap((Box<ColTypeOption>, Box<ColTypeOption>)),
}

/// User defined type.
/// [Read more...](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L608)
#[derive(Debug, Clone)]
pub struct CUdt {
    /// Keyspace name.
    pub ks: CString,
    /// UDT name
    pub udt_name: CString,
    /// List of pairs `(name, type)` where name is field name and type is type of field.
    pub descriptions: Vec<(CString, ColTypeOption)>,
}

impl FromCursor for CUdt {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<CUdt> {
        let ks = CString::from_cursor(&mut cursor)?;
        let udt_name = CString::from_cursor(&mut cursor)?;
        let n = try_from_bytes(cursor_next_value(&mut cursor, SHORT_LEN as u64)?.as_slice())?;
        let mut descriptions = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let name = CString::from_cursor(&mut cursor)?;
            let col_type = ColTypeOption::from_cursor(&mut cursor)?;
            descriptions.push((name, col_type));
        }

        Ok(CUdt {
               ks: ks,
               udt_name: udt_name,
               descriptions: descriptions,
           })
    }
}

/// User defined type.
/// [Read more...](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L608)
#[derive(Debug, Clone)]
pub struct CTuple {
    /// List of types.
    pub types: Vec<ColTypeOption>,
}

impl FromCursor for CTuple {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<CTuple> {
        let n = try_from_bytes(cursor_next_value(&mut cursor, SHORT_LEN as u64)?.as_slice())?;
        let mut types = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let col_type = ColTypeOption::from_cursor(&mut cursor)?;
            types.push(col_type);
        }

        Ok(CTuple { types: types })
    }
}

/// The structure represents a body of a response frame of type `prepared`
#[derive(Debug)]
pub struct BodyResResultPrepared {
    /// id of prepared request
    pub id: CBytesShort,
    /// metadata
    pub metadata: PreparedMetadata,
    /// It is defined exactly the same as <metadata> in the Rows
    /// documentation.
    pub result_metadata: RowsMetadata,
}

impl FromCursor for BodyResResultPrepared {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultPrepared> {
        let id = CBytesShort::from_cursor(&mut cursor)?;
        let metadata = PreparedMetadata::from_cursor(&mut cursor)?;
        let result_metadata = RowsMetadata::from_cursor(&mut cursor)?;

        Ok(BodyResResultPrepared {
               id: id,
               metadata: metadata,
               result_metadata: result_metadata,
           })
    }
}

/// The structure that represents metadata of prepared response.
#[derive(Debug)]
pub struct PreparedMetadata {
    pub flags: i32,
    pub columns_count: i32,
    pub pk_count: i32,
    pub pk_indexes: Vec<i16>,
    pub global_table_spec: Option<(CString, CString)>,
    pub col_specs: Vec<ColSpec>,
}

impl FromCursor for PreparedMetadata {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<PreparedMetadata> {
        let flags = CInt::from_cursor(&mut cursor)?;
        let columns_count = CInt::from_cursor(&mut cursor)?;
        let pk_count = if cfg!(feature = "v3") {
            0
        } else {
            // v4 or v5
            CInt::from_cursor(&mut cursor)?
        };
        let pk_index_results: Vec<Option<i16>> = (0..pk_count)
            .map(|_| {
                     cursor_next_value(&mut cursor, SHORT_LEN as u64)
                         .ok()
                         .and_then(|b| try_i16_from_bytes(b.as_slice()).ok())
                 })
            .collect();

        let pk_indexes: Vec<i16> = if pk_index_results.iter().any(Option::is_none) {
            return Err("pk indexes error".into());
        } else {
            pk_index_results
                .iter()
                .cloned()
                .map(|r| r.unwrap())
                .collect()
        };
        let mut global_table_space: Option<(CString, CString)> = None;
        let has_global_table_space = RowsMetadataFlag::has_global_table_space(flags);
        if has_global_table_space {
            let keyspace = CString::from_cursor(&mut cursor)?;
            let tablename = CString::from_cursor(&mut cursor)?;
            global_table_space = Some((keyspace, tablename))
        }
        let col_specs = ColSpec::parse_colspecs(&mut cursor, columns_count, has_global_table_space);

        Ok(PreparedMetadata {
               flags: flags,
               columns_count: columns_count,
               pk_count: pk_count,
               pk_indexes: pk_indexes,
               global_table_spec: global_table_space,
               col_specs: col_specs,
           })
    }
}
