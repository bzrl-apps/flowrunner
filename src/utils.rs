use serde_json::value::Value as jsonValue;
use serde_json::{Number, Map, Value};
use serde_yaml::Value as yamlValue;

use anyhow::{anyhow, Result};

use log::debug;

use envmnt::{ExpandOptions, ExpansionType};
use tera::{Tera, Context};

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
    } else if v.is_mapping() {
        if let Some(map) = v.as_mapping() {
            let mut object = Map::new();

            for (k, v) in map.into_iter() {
                //let key = convert_value_yaml_to_json(k)?.as_str().ok_or(anyhow!("key cannot convert to json string"))?;
                object.insert(convert_value_yaml_to_json(k)?.as_str().ok_or(anyhow!("key cannot convert to json string"))?.to_string(), convert_value_yaml_to_json(v)?);
            }

            val = jsonValue::Object(object);
        }
    }

    Ok(val)
}

pub fn expand_env_map(m: &mut Map<String, Value>) {
    for (k, v) in m.clone().into_iter() {
        m.insert(k, expand_env_value(&v));
    }
}

pub fn expand_env_value(value: &Value) -> Value {
    let mut options = ExpandOptions::new();
    options.expansion_type = Some(ExpansionType::UnixBracketsWithDefaults);

    match value {
        Value::Array(arr) => {
            let mut v: Vec<Value> = vec![];
            for e in arr.into_iter() {
                let expanded_v = expand_env_value(e);
                v.push(expanded_v);
            }

            return Value::Array(v);
        },
        Value::Object(map) => {
            let mut m: Map<String, Value> = Map::new();
            for (k, v) in map.into_iter() {
                let expanded_v = expand_env_value(v);
                m.insert(k.to_string(), expanded_v);
            }

            return Value::Object(m);
        },
        Value::Null |
        Value::Bool(_) |
        Value::Number(_) => value.to_owned(),
        Value::String(s) => {
            return Value::String(envmnt::expand(s, Some(options)));
        }
    }
}

pub fn render_param_template(component: &str, key: &str, value: &Value, data: &Map<String, Value>) -> Result<Value> {
    debug!("Rendering param templating: component {}, key {}, value {:?}, data {:?}", component, key, value, data);

    let mut tera = Tera::default();
    let exp_env_v = expand_env_value(value);

    let context = match Context::from_value(Value::Object(data.to_owned())) {
        Ok(c) => c,
        Err(e) => return Err(anyhow!(e)),
    };

    match exp_env_v {
        Value::Array(arr) => {
            let mut v: Vec<Value> = vec![];
            for e in arr.into_iter() {
                let rendered_v = render_param_template(component, key, &e, data)?;
                v.push(rendered_v);
            }

            return Ok(Value::Array(v));
        },
        Value::Object(map) => {
            let mut m: Map<String, Value> = Map::new();
            for (k, v) in map.into_iter() {
                let rendered_v = render_param_template(component, key, &v, data)?;
                m.insert(k.to_string(), rendered_v);
            }

            return Ok(Value::Object(m));
        },
        Value::Null |
        Value::Bool(_) |
        Value::Number(_) => Ok(exp_env_v.to_owned()),
        Value::String(s) => {
            match tera.render_str(s.as_str(), &context) {
                Ok(s) => Ok(Value::String(s)),
                Err(e) => Err(anyhow!(e))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

    #[test]
    fn test_expand_env_map() {
        env_logger::init();

        let input = json!({
            "var1": "$VAR1",
            "var2": [
                "1",
                "$VAR21",
                "3",
            ],
            "var3": [
                "var31",
                "$VAR32",
                "var33",
            ],
            "var4": {
                "var41": {
                    "var411": "var411",
                    "var412": "${VAR412}"
                },
                "var42": "var42",
                "var43": "$VAR43"
            }
        });

        envmnt::set("VAR1", "var1");
        envmnt::set("VAR21", "2");
        envmnt::set("VAR32", "var32");
        envmnt::set("VAR412", "var412");
        envmnt::set("VAR43", "var43");

        let mut expanded_m = input.as_object().unwrap().to_owned();
        expand_env_map(&mut expanded_m);

        let expected = json!({
            "var1": "var1",
            "var2": [
                "1",
                "2",
                "3",
            ],
            "var3": [
                "var31",
                "var32",
                "var33",
            ],
            "var4": {
                "var41": {
                    "var411": "var411",
                    "var412": "var412"
                },
                "var42": "var42",
                "var43": "var43"
            }
        });

        assert_eq!(expected.as_object().unwrap(), &expanded_m);
    }
}
