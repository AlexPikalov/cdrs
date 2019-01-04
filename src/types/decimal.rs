use super::{to_int, to_varint};
use frame::traits::IntoBytes;

/// Cassandra Decimal type
#[derive(Debug, Clone, PartialEq)]
pub struct Decimal(f64);

impl Decimal {
  /// Method that returns plain `f64` value.
  pub fn as_plain(&self) -> f64 {
    self.0
  }
}

impl IntoBytes for Decimal {
  fn into_cbytes(&self) -> Vec<u8> {
    let mut scale: i32 = 0;
    let mut unscaled: f64 = self.0;

    loop {
      unscaled = 10f64.powi(scale) * self.0;
      if unscaled.trunc() == unscaled {
        break;
      }
      scale += 1;
    }

    let mut bytes: Vec<u8> = vec![];
    bytes.extend(to_int(scale));
    bytes.extend(to_varint(unscaled as i64));

    bytes
  }
}

macro_rules! impl_from_for_decimal {
  ($t:ty) => {
    impl From<$t> for Decimal {
      fn from(i: $t) -> Self {
        Decimal(i.into())
      }
    }
  };
}

impl_from_for_decimal!(i8);
impl_from_for_decimal!(i16);
impl_from_for_decimal!(i32);
impl_from_for_decimal!(u8);
impl_from_for_decimal!(u16);
impl_from_for_decimal!(f32);
impl_from_for_decimal!(f64);

#[cfg(test)]
mod test {
  use super::*;
  #[test]
  fn into_cbytes_test() {
    let int_positive: f32 = 129.0;
    assert_eq!(
      Decimal::from(int_positive).into_cbytes(),
      vec![0, 0, 0, 0, 0x00, 0x81]
    );

    let int_negative: f32 = -129.0;
    assert_eq!(
      Decimal::from(int_negative).into_cbytes(),
      vec![0, 0, 0, 0, 0xFF, 0x7F]
    );

    let float_positive: f32 = 12.9;
    let expected: Vec<u8> = vec![0, 0, 0, 1, 0x00, 0x81];
    assert_eq!(Decimal::from(float_positive).into_cbytes(), expected);

    let float_negative: f32 = -12.9;
    let expected: Vec<u8> = vec![0, 0, 0, 1, 0xFF, 0x7F];
    assert_eq!(Decimal::from(float_negative).into_cbytes(), expected);
  }
}
