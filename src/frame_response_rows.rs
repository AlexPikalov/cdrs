use std::io::{Cursor, Read};
use super::{IntoBytes, FromBytes, FromCursor};
use super::types::*;

pub struct BodyResResultRows {
    pub metadata: RowsMetadata,
    pub rows_count: i32,
    /// From spec: it is composed of <row_1>...<row_m> where m is <rows_count>.
    /// Each <row_i> is composed of <value_1>...<value_n> where n is
    /// <columns_count> and where <value_j> is a [bytes] representing the value
    /// returned for the jth column of the ith row.
    pub rows_content: Vec<Vec<CBytes>>
}

impl BodyResResultRows {
    fn get_rows_content(mut cursor: &mut Cursor<Vec<u8>>, rows_count: i32, columns_count: i32) -> Vec<Vec<CBytes>> {
        let mut v: Vec<Vec<CBytes>> = Vec::new();
        for _ in 0..rows_count {
            let mut row: Vec<CBytes> = Vec::new();
            for _ in 0..columns_count {
                row.push(Vec::from_cursor(&mut cursor) as CBytes);
            }
            v.push(row);
        }
        return v;
    }
}

impl FromCursor for BodyResResultRows {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResResultRows{
        let metadata = RowsMetadata::from_cursor(&mut cursor);
        let mut rows_count_bytes = [0; INT_LEN];
        if let Err(err) = cursor.read(&mut rows_count_bytes) {
            error!("Read Cassandra rows column count: {}", err);
            panic!(err);
        }
        let rows_count: i32 = from_bytes(rows_count_bytes.to_vec()) as i32;
        let rows_content: Vec<Vec<CBytes>> = BodyResResultRows::get_rows_content(&mut cursor, rows_count, metadata.columns_count);
        return BodyResResultRows {
            metadata: metadata,
            rows_count: rows_count,
            rows_content: rows_content
        };
    }
}

pub struct RowsMetadata {
    pub flags: i32,
    pub columns_count: i32,
    pub paging_state: Option<Vec<u8>>,
    // In fact by specification Vec should have only two elements representing the
    // (unique) keyspace name and table name the columns belong to
    pub global_table_space: Option<Vec<String>>,
    pub col_specs: Vec<ColSpec>,
}

impl FromCursor for RowsMetadata {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> RowsMetadata {
        // let mut cursor = Cursor::new(bytes);
        let mut flags_bytes = [0; INT_LEN];
        let mut columns_count_bytes = [0; INT_LEN];

        // NOTE: order of reads does matter
        if let Err(err) = cursor.read(&mut flags_bytes) {
            error!("Read Cassandra rows metadata flag: {}", err);
            panic!(err);
        }
        if let Err(err) = cursor.read(&mut columns_count_bytes) {
            error!("Read Cassandra rows metadata column count: {}", err);
            panic!(err);
        }

        let flags: i32 = from_bytes(flags_bytes.to_vec()) as i32;
        let columns_count: i32 = from_bytes(columns_count_bytes.to_vec()) as i32;

        let mut paging_state: Option<Vec<u8>> = None;
        if RowsMetadataFlag::has_has_more_pages(flags) {
            paging_state = Some(Vec::from_cursor(&mut cursor))
        }

        let mut global_table_space: Option<Vec<String>> = None;
        let has_global_table_space = RowsMetadataFlag::has_global_table_space(flags);
        if has_global_table_space {
            let keyspace = String::from_cursor(&mut cursor);
            let tablename = String::from_cursor(&mut cursor);
            global_table_space = Some(vec![keyspace, tablename])
        }

        let col_specs = ColSpec::parse_colspecs(&mut cursor, columns_count, has_global_table_space);

        return RowsMetadata {
            flags: flags,
            columns_count: columns_count,
            paging_state: paging_state,
            global_table_space: global_table_space,
            col_specs: col_specs
        }
    }
}

const GLOBAL_TABLE_SPACE: i32 = 0x0001;
const HAS_MORE_PAGES: i32 = 0x0002;
const NO_METADATA: i32 = 0x0004;

pub enum RowsMetadataFlag {
    GlobalTableSpace,
    HasMorePages,
    NoMetadata
}

impl RowsMetadataFlag {
    /// Shows if provided flag contains GlobalTableSpace rows metadata flag
    pub fn has_global_table_space(flag: i32) -> bool {
        return (flag & GLOBAL_TABLE_SPACE) != 0;
    }

    /// Sets GlobalTableSpace rows metadata flag
    pub fn set_global_table_space(flag: i32) -> i32 {
        return flag | GLOBAL_TABLE_SPACE;
    }

    /// Shows if provided flag contains HasMorePages rows metadata flag
    pub fn has_has_more_pages(flag: i32) -> bool {
        return (flag & HAS_MORE_PAGES) != 0;
    }

    /// Sets HasMorePages rows metadata flag
    pub fn set_has_more_pages(flag: i32) -> i32 {
        return flag | HAS_MORE_PAGES;
    }

    /// Shows if provided flag contains NoMetadata rows metadata flag
    pub fn has_no_metadata(flag: i32) -> bool {
        return (flag & NO_METADATA) != 0;
    }

    /// Sets NoMetadata rows metadata flag
    pub fn set_no_metadata(flag: i32) -> i32 {
        return flag | NO_METADATA;
    }
}

impl IntoBytes for RowsMetadataFlag {
    fn into_cbytes(&self) -> Vec<u8> {
        return match *self {
            RowsMetadataFlag::GlobalTableSpace => to_int(GLOBAL_TABLE_SPACE as i64),
            RowsMetadataFlag::HasMorePages => to_int(HAS_MORE_PAGES as i64),
            RowsMetadataFlag::NoMetadata => to_int(NO_METADATA as i64)
        };
    }
}

impl FromBytes for RowsMetadataFlag {
    fn from_bytes(bytes: Vec<u8>) -> RowsMetadataFlag {
        return match from_bytes(bytes.clone()) as i32 {
            GLOBAL_TABLE_SPACE => RowsMetadataFlag::GlobalTableSpace,
            HAS_MORE_PAGES => RowsMetadataFlag::HasMorePages,
            NO_METADATA => RowsMetadataFlag::NoMetadata,
            _ => {
                error!("Unexpected Cassandra rows metadata flag: {:?}", bytes);
                panic!("Unexpected Cassandra rows metadata flag: {:?}", bytes);
            }
        };
    }
}

pub struct ColSpec {
    /// The initial <ksname> is a [string] and is only present
    /// if the Global_tables_spec flag is NOT set
    pub ksname: Option<String>,
    /// The initial <tablename> is a [string] and is present
    /// if the Global_tables_spec flag is NOT set
    pub tablename: Option<String>,
    /// Column name
    pub name: String,
    /// Column type defined in spec in 4.2.5.2
    pub col_type: ColType
}

impl ColSpec {
    /// parse_colspecs tables mutable cursor, number of columns (column_count) and flags that indicates
    /// if Global_tables_spec is specified. It returns column_count of ColSpecs.
    pub fn parse_colspecs(mut cursor: &mut Cursor<Vec<u8>>,
        column_count: i32,
        with_globale_table_spec: bool) -> Vec<ColSpec> {
            let mut v: Vec<ColSpec> = vec![];

            for _ in 0..column_count {
                let mut ksname: Option<String> = None;

                let mut tablename: Option<String> = None;
                if !with_globale_table_spec {
                    ksname = Some(String::from_cursor(&mut cursor));
                    tablename = Some(String::from_cursor(&mut cursor));
                }

                let name = String::from_cursor(&mut cursor);

                let mut col_type_bytes = [0; INT_LEN];
                if let Err(err) = cursor.read(&mut col_type_bytes) {
                    error!("Read Cassandra column type error: {}", err);
                    panic!(err);
                }

                let col_type = ColType::from_bytes(col_type_bytes.to_vec());

                v.push(ColSpec {
                    ksname: ksname,
                    tablename: tablename,
                    name: name,
                    col_type: col_type
                });
            }

            return v;
        }
}

pub enum ColType {
    Custom,
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Cunter,
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
    Tuple
}

impl FromBytes for ColType {
    fn from_bytes(bytes: Vec<u8>) -> ColType {
        return match from_bytes(bytes.clone()) {
            0x0000 => ColType::Custom,
            0x0001 => ColType::Ascii,
            0x0002 => ColType::Bigint,
            0x0003 => ColType::Blob,
            0x0004 => ColType::Boolean,
            0x0005 => ColType::Cunter,
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
            _ => {
                error!("Unexpected Cassandra column type: {:?}", bytes);
                panic!("Unexpected Cassandra column type: {:?}", bytes);
            }
        };
    }
}
