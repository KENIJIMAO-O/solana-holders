use serde::{Serialize, Deserialize, Serializer, Deserializer};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use std::ops::{Add, Sub};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub struct ClickhouseDecimal(i128);

impl ClickhouseDecimal {
    /// 从 Decimal 创建
    pub fn from_decimal(d: Decimal) -> Self {
        let scaled: Decimal = d * Decimal::from_i64(1_000_000_000_000_i64).unwrap();
        ClickhouseDecimal(scaled.to_i128().unwrap_or(0))
    }

    /// 从整数创建（例如 -1 表示 -1.0）
    pub fn from_int(i: i64) -> Self {
        ClickhouseDecimal(i as i128 * 1_000_000_000_000)
    }

    /// 从浮点数创建
    pub fn from_f64(f: f64) -> Self {
        ClickhouseDecimal((f * 1_000_000_000_000.0) as i128)
    }

    /// 转换回 Decimal
    pub fn to_decimal(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.0, 12)
    }
}

impl Serialize for ClickhouseDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ClickhouseDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let value = i128::deserialize(deserializer)?;
        Ok(ClickhouseDecimal(value))
    }
}

// 实现加法
impl Add for ClickhouseDecimal {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        ClickhouseDecimal(self.0 + rhs.0)
    }
}

// 实现减法
impl Sub for ClickhouseDecimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        ClickhouseDecimal(self.0 - rhs.0)
    }
}

// 实现 Display trait，自动获得 to_string() 方法
impl fmt::Display for ClickhouseDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_decimal())
    }
}

// 实现 FromStr trait，支持从字符串解析
impl FromStr for ClickhouseDecimal {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decimal = Decimal::from_str(s)?;
        Ok(ClickhouseDecimal::from_decimal(decimal))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_plus_clickhouse_decimal(){
        let a = ClickhouseDecimal::from_f64(-454545.0);
        let b = ClickhouseDecimal::from_f64(454545.0);

        let c = a + b;
        println!("c:{:?}", c);
    }

    #[test]
    fn test_to_string() {
        // 测试正数
        let a = ClickhouseDecimal::from_f64(123.456);
        println!("a.to_string(): {}", a.to_string());
        assert_eq!(a.to_string(), "123.456");

        // 测试负数
        let b = ClickhouseDecimal::from_int(-1);
        println!("b.to_string(): {}", b.to_string());
        assert_eq!(b.to_string(), "-1");

        // 测试小数
        let c = ClickhouseDecimal::from_decimal(Decimal::from_f64(0.123456789012).unwrap());
        println!("c.to_string(): {}", c.to_string());

        // 测试零
        let d = ClickhouseDecimal::from_int(0);
        println!("d.to_string(): {}", d.to_string());
        assert_eq!(d.to_string(), "0");
    }

    #[test]
    fn test_from_str() {
        // 测试正数
        let a = ClickhouseDecimal::from_str("123.456").unwrap();
        println!("parsed '123.456': {}", a.to_string());
        assert_eq!(a.to_string(), "123.456");

        // 测试负数
        let b = ClickhouseDecimal::from_str("-999.99").unwrap();
        println!("parsed '-999.99': {}", b.to_string());
        assert_eq!(b.to_string(), "-999.99");

        // 测试整数
        let c = ClickhouseDecimal::from_str("100").unwrap();
        println!("parsed '100': {}", c.to_string());
        assert_eq!(c.to_string(), "100");

        // 测试零
        let d = ClickhouseDecimal::from_str("0").unwrap();
        println!("parsed '0': {}", d.to_string());
        assert_eq!(d.to_string(), "0");

        // 测试小数
        let e = ClickhouseDecimal::from_str("0.000000000001").unwrap();
        println!("parsed '0.000000000001': {}", e.to_string());
        assert_eq!(e.to_string(), "0.000000000001");

        // 测试往返转换（round-trip）
        let original = "12345.6789";
        let parsed = ClickhouseDecimal::from_str(original).unwrap();
        assert_eq!(parsed.to_string(), original);
        println!("✅ round-trip test passed: {}", original);

        // 测试错误情况
        let invalid = ClickhouseDecimal::from_str("invalid");
        assert!(invalid.is_err());
        println!("✅ invalid string correctly rejected");
    }
}