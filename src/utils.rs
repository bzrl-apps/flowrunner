use serde_json::value::Value as jsonValue;
use serde_json::Number;
use serde_yaml::Value as yamlValue;

use anyhow::{anyhow, Result};

pub fn convert_value_yaml_to_json(v: &yamlValue) -> Result<jsonValue> {
    let mut val = jsonValue::Null;

    if v.is_bool() {
        val = jsonValue::Bool(v.as_bool().ok_or(anyhow!("cannot convert to bool"))?);
    } else if v.is_f64() {
        val = jsonValue::Number(Number::from_f64(v.as_f64().ok_or(anyhow!("cannot convert to f64"))?).ok_or(anyhow!("cannot convert from f64"))?);
    } else if v.is_u64() {
        val = jsonValue::Number(Number::from(v.as_u64().ok_or(anyhow!("cannot convert to u64"))?));
    } else if v.is_i64() {
        val = jsonValue::Number(Number::from(v.as_i64().ok_or(anyhow!("cannot convert to i64"))?));
    } else if v.is_string() {
        val = jsonValue::String(v.as_str().ok_or(anyhow!("cannot convert to string"))?.to_string());
    } else if v.is_sequence() {
        // recursive
        if let Some(seq) = v.as_sequence() {
            let mut array: Vec<jsonValue> = Vec::new();

            for s in seq.iter() {
                array.push(convert_value_yaml_to_json(s)?);
            }

            val = jsonValue::Array(array);
        }
    }

    Ok(val)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    #[test]
    fn test_convert_value_yaml_to_json() {
        let mut val = yamlValue::Bool(false);

        assert_eq!(val.as_bool().unwrap(), false);

        val = yamlValue::Number(serde_yaml::Number::from(12.46));
        assert_eq!(val.as_f64().unwrap(), 12.46);


        val = yamlValue::Number(serde_yaml::Number::from(1556));
        assert_eq!(val.as_i64().unwrap(), 1556);
        assert_eq!(val.as_u64().unwrap(), 1556);

        val = yamlValue::String("hello world".to_string());
        assert_eq!(val.as_str().unwrap(), "hello world");
    }
}
