use super::{to_int, to_varint};
use crate::frame::traits::IntoBytes;

/// Cassandra Decimal type
#[derive(Debug, Clone, PartialEq)]
pub struct Decimal {
  pub unscaled: i64,
  pub scale: u32,
}

impl Decimal {
  pub fn new(unscaled: i64, scale: u32) -> Self {
    Decimal { unscaled, scale }
  }

  /// Method that returns plain `f64` value.
  pub fn as_plain(&self) -> f64 {
    (self.unscaled as f64) / (10i64.pow(self.scale) as f64)
  }
}

impl IntoBytes for Decimal {
  fn into_cbytes(&self) -> Vec<u8> {
    let mut bytes: Vec<u8> = vec![];
    bytes.extend(to_int(self.scale as i32));
    bytes.extend(to_varint(self.unscaled));

    bytes
  }
}

macro_rules! impl_from_for_decimal {
  ($t:ty) => {
    impl From<$t> for Decimal {
      fn from(i: $t) -> Self {
        Decimal {
          unscaled: i as i64,
          scale: 0,
        }
      }
    }
  };
}

impl_from_for_decimal!(i8);
impl_from_for_decimal!(i16);
impl_from_for_decimal!(i32);
impl_from_for_decimal!(i64);
impl_from_for_decimal!(u8);
impl_from_for_decimal!(u16);

impl From<f32> for Decimal {
  fn from(f: f32) -> Decimal {
    let mut scale: u32 = 0;

    loop {
      let unscaled = f * (10i64.pow(scale) as f32);

      if unscaled == unscaled.trunc() {
        return Decimal::new(unscaled as i64, scale);
      }

      scale += 1;
    }
  }
}

impl From<f64> for Decimal {
  fn from(f: f64) -> Decimal {
    let mut scale: u32 = 0;

    loop {
      let unscaled = f * (10i64.pow(scale) as f64);

      if unscaled == unscaled.trunc() {
        return Decimal::new(unscaled as i64, scale);
      }

      scale += 1;
    }
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn into_cbytes_test() {
    assert_eq!(
      Decimal::new(129, 0).into_cbytes(),
      vec![0, 0, 0, 0, 0x00, 0x81]
    );

    assert_eq!(
      Decimal::new(-129, 0).into_cbytes(),
      vec![0, 0, 0, 0, 0xFF, 0x7F]
    );

    let expected: Vec<u8> = vec![0, 0, 0, 1, 0x00, 0x81];
    assert_eq!(Decimal::new(129, 1).into_cbytes(), expected);

    let expected: Vec<u8> = vec![0, 0, 0, 1, 0xFF, 0x7F];
    assert_eq!(Decimal::new(-129, 1).into_cbytes(), expected);
  }

  #[test]
  fn from_f32() {
    assert_eq!(Decimal::from(12300001 as f32), Decimal::new(12300001, 0));
    assert_eq!(Decimal::from(1230000.1 as f32), Decimal::new(12300001, 1));
    assert_eq!(Decimal::from(0.12300001 as f32), Decimal::new(12300001, 8));
  }

  #[test]
  fn from_f64() {
    assert_eq!(
      Decimal::from(1230000000000001i64 as f64),
      Decimal::new(1230000000000001i64, 0)
    );
    assert_eq!(
      Decimal::from(123000000000000.1f64),
      Decimal::new(1230000000000001i64, 1)
    );
    assert_eq!(
      Decimal::from(0.1230000000000001f64),
      Decimal::new(1230000000000001i64, 16)
    );
  }
}
