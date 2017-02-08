use std::io::Cursor;
use {IntoBytes, FromBytes, FromCursor};
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
    fn from_bytes(bytes: Vec<u8>) -> ResultKind {
        match from_bytes(bytes.clone()) {
            0x0001 => ResultKind::Void,
            0x0002 => ResultKind::Rows,
            0x0003 => ResultKind::SetKeyspace,
            0x0004 => ResultKind::Prepared,
            0x0005 => ResultKind::SchemaChange,
            _ => unreachable!(),
        }
    }
}

impl FromCursor for ResultKind {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> ResultKind {
        ResultKind::from_bytes(cursor_next_value(&mut cursor, INT_LEN as u64))
    }
}

/// ResponseBody is a generalized enum that represents all types of responses. Each of enum
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
    fn parse_body_from_cursor(mut cursor: &mut Cursor<Vec<u8>>,
                              result_kind: ResultKind)
                              -> ResResultBody {
        match result_kind {
            ResultKind::Void => ResResultBody::Void(BodyResResultVoid::from_cursor(&mut cursor)),
            ResultKind::Rows => ResResultBody::Rows(BodyResResultRows::from_cursor(&mut cursor)),
            ResultKind::SetKeyspace => {
                ResResultBody::SetKeyspace(BodyResResultSetKeyspace::from_cursor(&mut cursor))
            }
            ResultKind::Prepared => {
                ResResultBody::Prepared(BodyResResultPrepared::from_cursor(&mut cursor))
            }
            ResultKind::SchemaChange => {
                ResResultBody::SchemaChange(SchemaChange::from_cursor(&mut cursor))
            }


        }
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> ResResultBody {
        let result_kind = ResultKind::from_cursor(&mut cursor);
        ResResultBody::parse_body_from_cursor(&mut cursor, result_kind)
    }
}

/// Body of a response of type Void
#[derive(Debug)]
pub struct BodyResResultVoid {}

/// Empty result body.
impl BodyResResultVoid {
    pub fn new() -> BodyResResultVoid {
        BodyResResultVoid {}
    }
}

impl FromBytes for BodyResResultVoid {
    fn from_bytes(_bytes: Vec<u8>) -> BodyResResultVoid {
        // as it's empty by definition just create BodyResVoid
        BodyResResultVoid::new()
    }
}

impl FromCursor for BodyResResultVoid {
    fn from_cursor(mut _cursor: &mut Cursor<Vec<u8>>) -> BodyResResultVoid {
        BodyResResultVoid::new()
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResResultSetKeyspace {
        BodyResResultSetKeyspace::new(CString::from_cursor(&mut cursor))
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
    fn get_rows_content(mut cursor: &mut Cursor<Vec<u8>>,
                        rows_count: i32,
                        columns_count: i32)
                        -> Vec<Vec<CBytes>> {
        (0..rows_count)
            .map(|_| {
                return (0..columns_count)
                    .map(|_| CBytes::from_cursor(&mut cursor) as CBytes)
                    .collect();
            })
            .collect()
    }

    /// Returns a list of tuples `(CBytes, ColType)` with value and type of values respectively.
    /// `n` is a number of row.
    pub fn nth_row_val_types(&self, n: usize) -> Vec<(CBytes, ColType)> {
        let col_types = self.metadata
            .col_specs
            .iter()
            .map(|col_spec| col_spec.col_type.id.clone());
        self.rows_content[n]
            .iter()
            .map(|cbyte| cbyte.clone())
            .zip(col_types)
            .collect()
    }
}

impl FromCursor for BodyResResultRows {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResResultRows {
        let metadata = RowsMetadata::from_cursor(&mut cursor);
        let rows_count = CInt::from_cursor(&mut cursor);
        let rows_content: Vec<Vec<CBytes>> =
            BodyResResultRows::get_rows_content(&mut cursor, rows_count, metadata.columns_count);
        BodyResResultRows {
            metadata: metadata,
            rows_count: rows_count,
            rows_content: rows_content,
        }
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> RowsMetadata {
        let flags = CInt::from_cursor(&mut cursor);
        let columns_count = CInt::from_cursor(&mut cursor);

        let mut paging_state: Option<CBytes> = None;
        if RowsMetadataFlag::has_has_more_pages(flags) {
            paging_state = Some(CBytes::from_cursor(&mut cursor))
        }

        let mut global_table_space: Option<Vec<CString>> = None;
        let has_global_table_space = RowsMetadataFlag::has_global_table_space(flags);
        if has_global_table_space {
            let keyspace = CString::from_cursor(&mut cursor);
            let tablename = CString::from_cursor(&mut cursor);
            global_table_space = Some(vec![keyspace, tablename])
        }

        let col_specs = ColSpec::parse_colspecs(&mut cursor, columns_count, has_global_table_space);

        RowsMetadata {
            flags: flags,
            columns_count: columns_count,
            paging_state: paging_state,
            global_table_space: global_table_space,
            col_specs: col_specs,
        }
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
            RowsMetadataFlag::GlobalTableSpace => to_int(GLOBAL_TABLE_SPACE as i64),
            RowsMetadataFlag::HasMorePages => to_int(HAS_MORE_PAGES as i64),
            RowsMetadataFlag::NoMetadata => to_int(NO_METADATA as i64),
        }
    }
}

impl FromBytes for RowsMetadataFlag {
    fn from_bytes(bytes: Vec<u8>) -> RowsMetadataFlag {
        match from_bytes(bytes.clone()) as i32 {
            GLOBAL_TABLE_SPACE => RowsMetadataFlag::GlobalTableSpace,
            HAS_MORE_PAGES => RowsMetadataFlag::HasMorePages,
            NO_METADATA => RowsMetadataFlag::NoMetadata,
            _ => {
                error!("Unexpected Cassandra rows metadata flag: {:?}", bytes);
                panic!("Unexpected Cassandra rows metadata flag: {:?}", bytes);
            }
        }
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
    pub fn parse_colspecs(mut cursor: &mut Cursor<Vec<u8>>,
                          column_count: i32,
                          with_globale_table_spec: bool)
                          -> Vec<ColSpec> {
        (0..column_count)
            .map(|_| {
                let mut ksname: Option<CString> = None;
                let mut tablename: Option<CString> = None;
                if !with_globale_table_spec {
                    ksname = Some(CString::from_cursor(&mut cursor));
                    tablename = Some(CString::from_cursor(&mut cursor));
                }
                let name = CString::from_cursor(&mut cursor);
                let col_type = ColTypeOption::from_cursor(&mut cursor);

                return ColSpec {
                    ksname: ksname,
                    tablename: tablename,
                    name: name,
                    col_type: col_type,
                };
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
    fn from_bytes(bytes: Vec<u8>) -> ColType {
        match from_bytes(bytes.clone()) {
            0x0000 => ColType::Custom,
            0x0001 => ColType::Ascii,
            0x0002 => ColType::Bigint,
            0x0003 => ColType::Blob,
            0x0004 => ColType::Boolean,
            0x0005 => ColType::Counter,
            0x0006 => ColType::Decimal,
            0x0007 => ColType::Double,
            0x0008 => ColType::Float,
            0x0009 => ColType::Int,
            0x000B => ColType::Timestamp,
            0x000C => ColType::Uuid,
            0x000D => ColType::Varchar,
            0x000E => ColType::Varint,
            0x000F => ColType::Timeuuid,
            0x0010 => ColType::Inet,
            0x0011 => ColType::Date,
            0x0012 => ColType::Time,
            0x0013 => ColType::Smallint,
            0x0014 => ColType::Tinyint,
            0x0020 => ColType::List,
            0x0021 => ColType::Map,
            0x0022 => ColType::Set,
            0x0030 => ColType::Udt,
            0x0031 => ColType::Tuple,
            _ => unreachable!(),
        }
    }
}

impl FromCursor for ColType {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> ColType {
        let option_id_bytes = cursor_next_value(&mut cursor, SHORT_LEN as u64);
        let col_type = ColType::from_bytes(option_id_bytes);
        col_type
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> ColTypeOption {
        let id = ColType::from_cursor(&mut cursor);
        let value = match id {
            ColType::Custom => Some(ColTypeOptionValue::CString(CString::from_cursor(&mut cursor))),
            ColType::Set => {
                let col_type = ColTypeOption::from_cursor(&mut cursor);
                Some(ColTypeOptionValue::CSet(Box::new(col_type)))
            }
            ColType::List => {
                let col_type = ColTypeOption::from_cursor(&mut cursor);
                Some(ColTypeOptionValue::CList(Box::new(col_type)))
            }
            ColType::Udt => Some(ColTypeOptionValue::UdtType(CUdt::from_cursor(&mut cursor))),
            ColType::Map => {
                let name_type = ColTypeOption::from_cursor(&mut cursor);
                let value_type = ColTypeOption::from_cursor(&mut cursor);
                Some(ColTypeOptionValue::CMap((Box::new(name_type), Box::new(value_type))))
            }
            _ => None,
        };

        ColTypeOption {
            id: id,
            value: value,
        }
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CUdt {
        let ks = CString::from_cursor(&mut cursor);
        let udt_name = CString::from_cursor(&mut cursor);
        let n = from_bytes(cursor_next_value(&mut cursor, SHORT_LEN as u64));
        let descriptions: Vec<(CString, ColTypeOption)> = (0..n)
            .map(|_| {
                let name = CString::from_cursor(&mut cursor);
                let col_type = ColTypeOption::from_cursor(&mut cursor);
                return (name, col_type);
            })
            .collect();

        CUdt {
            ks: ks,
            udt_name: udt_name,
            descriptions: descriptions,
        }
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResResultPrepared {
        let id = CBytesShort::from_cursor(&mut cursor);
        let metadata = PreparedMetadata::from_cursor(&mut cursor);
        let result_metadata = RowsMetadata::from_cursor(&mut cursor);

        BodyResResultPrepared {
            id: id,
            metadata: metadata,
            result_metadata: result_metadata,
        }
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
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> PreparedMetadata {
        let flags = CInt::from_cursor(&mut cursor);
        let columns_count = CInt::from_cursor(&mut cursor);
        let pk_count = CInt::from_cursor(&mut cursor);
        let pk_indexes: Vec<i16> = (0..pk_count).fold(vec![], |mut acc, _| {
            let idx = from_bytes(cursor_next_value(&mut cursor, SHORT_LEN as u64)) as i16;
            acc.push(idx);
            acc
        });
        let mut global_table_space: Option<(CString, CString)> = None;
        let has_global_table_space = RowsMetadataFlag::has_global_table_space(flags);
        if has_global_table_space {
            let keyspace = CString::from_cursor(&mut cursor);
            let tablename = CString::from_cursor(&mut cursor);
            global_table_space = Some((keyspace, tablename))
        }
        let col_specs = ColSpec::parse_colspecs(&mut cursor, columns_count, has_global_table_space);

        PreparedMetadata {
            flags: flags,
            columns_count: columns_count,
            pk_count: pk_count,
            pk_indexes: pk_indexes,
            global_table_spec: global_table_space,
            col_specs: col_specs,
        }
    }
}
