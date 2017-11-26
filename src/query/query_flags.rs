use frame::AsByte;

const FLAGS_VALUE: u8 = 0x01;
const FLAGS_SKIP_METADATA: u8 = 0x02;
const WITH_PAGE_SIZE: u8 = 0x04;
const WITH_PAGING_STATE: u8 = 0x08;
const WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const WITH_NAME_FOR_VALUES: u8 = 0x40;

/// Cassandra Query Flags.
#[derive(Clone, Debug)]
pub enum QueryFlags {
  /// If set indicates that Query Params contains value.
  Value,
  /// If set indicates that Query Params does not contain metadata.
  SkipMetadata,
  /// If set indicates that Query Params contains page size.
  PageSize,
  /// If set indicates that Query Params contains paging state.
  WithPagingState,
  /// If set indicates that Query Params contains serial consistency.
  WithSerialConsistency,
  /// If set indicates that Query Params contains default timestamp.
  WithDefaultTimestamp,
  /// If set indicates that Query Params values are named ones.
  WithNamesForValues,
}

impl QueryFlags {
  #[doc(hidden)]
  pub fn has_value(byte: u8) -> bool {
    (byte & FLAGS_VALUE) != 0
  }

  #[doc(hidden)]
  pub fn set_value(byte: u8) -> u8 {
    byte | FLAGS_VALUE
  }

  #[doc(hidden)]
  pub fn has_skip_metadata(byte: u8) -> bool {
    (byte & FLAGS_SKIP_METADATA) != 0
  }

  #[doc(hidden)]
  pub fn set_skip_metadata(byte: u8) -> u8 {
    byte | FLAGS_SKIP_METADATA
  }

  #[doc(hidden)]
  pub fn has_page_size(byte: u8) -> bool {
    (byte & WITH_PAGE_SIZE) != 0
  }

  #[doc(hidden)]
  pub fn set_page_size(byte: u8) -> u8 {
    byte | WITH_PAGE_SIZE
  }

  #[doc(hidden)]
  pub fn has_with_paging_state(byte: u8) -> bool {
    (byte & WITH_PAGING_STATE) != 0
  }

  #[doc(hidden)]
  pub fn set_with_paging_state(byte: u8) -> u8 {
    byte | WITH_PAGING_STATE
  }

  #[doc(hidden)]
  pub fn has_with_serial_consistency(byte: u8) -> bool {
    (byte & WITH_SERIAL_CONSISTENCY) != 0
  }

  #[doc(hidden)]
  pub fn set_with_serial_consistency(byte: u8) -> u8 {
    byte | WITH_SERIAL_CONSISTENCY
  }

  #[doc(hidden)]
  pub fn has_with_default_timestamp(byte: u8) -> bool {
    (byte & WITH_DEFAULT_TIMESTAMP) != 0
  }

  #[doc(hidden)]
  pub fn set_with_default_timestamp(byte: u8) -> u8 {
    byte | WITH_DEFAULT_TIMESTAMP
  }

  #[doc(hidden)]
  pub fn has_with_names_for_values(byte: u8) -> bool {
    (byte & WITH_NAME_FOR_VALUES) != 0
  }

  #[doc(hidden)]
  pub fn set_with_names_for_values(byte: u8) -> u8 {
    byte | WITH_NAME_FOR_VALUES
  }
}

impl AsByte for QueryFlags {
  fn as_byte(&self) -> u8 {
    match *self {
      QueryFlags::Value => FLAGS_VALUE,
      QueryFlags::SkipMetadata => FLAGS_SKIP_METADATA,
      QueryFlags::PageSize => WITH_PAGE_SIZE,
      QueryFlags::WithPagingState => WITH_PAGING_STATE,
      QueryFlags::WithSerialConsistency => WITH_SERIAL_CONSISTENCY,
      QueryFlags::WithDefaultTimestamp => WITH_DEFAULT_TIMESTAMP,
      QueryFlags::WithNamesForValues => WITH_NAME_FOR_VALUES,
    }
  }
}
