use log::*;
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
//use std::collections::HashMap;
use serde_json::Value;

use anyhow::{anyhow, Result};

#[macro_export]
macro_rules! json_map {
    ($( $key:expr => $val:expr), *) => {
        {
            let mut map = serde_json::Map::new();
            $(map.insert($key.to_string(), $val); )*

            map
        }
    }
}

#[derive(Debug, Default)]
pub struct JsonOps {
    value: Value,
}

impl JsonOps {
    pub fn new(value: Value) -> Self {
        JsonOps { value }
    }

    pub fn get_json_value(&self, path: &str) -> Value {
        self.get_value_by_path(path)
    }

    pub fn get_value<T: DeserializeOwned + Default>(&self, path: &str) -> T {
        let v = self.get_value_by_path(path);

        match serde_json::from_value(v) {
            Ok(v) => v,
            Err(_) => T::default(),
        }
    }

    pub fn get_value_e<T: DeserializeOwned + Default>(&self, path: &str) -> Result<T> {
        let v = self.get_value_by_path(path);

        match serde_json::from_value(v) {
            Ok(v) => Ok(v),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Return the value corresponding to a given path.
    ///
    /// If no value is found, it will return Value::Null.
    fn get_value_by_path(&self, path: &str) -> Value {
        let mut val = self.value.clone();

        if path.is_empty() {
            return val;
        }

        let slice_path: Vec<&str> = path.split('.').collect();

        for k in slice_path.iter() {
            let val_type = get_value_type(&val);
            val = match k.parse::<usize>() {
                Ok(n) => {
                    // n can be an integer.
                    // So if val is an array, val[n] is an element of array
                    if val_type.as_str() == "array" {
                        val[n].clone()
                    } else { // Otherwise, it is a just an element of a object so string
                        val[k].clone()
                    }
                },
                Err(_) => val[k].clone(),
            };

            if val == Value::Null {
                return val;
            }
        }

        val
    }

    /// Set new value to a member of the current value.
    ///
    /// Current value must be an object or array. In this case, if the path is empty, it will
    /// replace current value by the new one as it is if they are the same type.
    ///
    /// If the path is not empty, it will get and check the 1st index of the path. If the returned
    /// value is a simple type such as number, bool or string, it will replace if both values are
    /// the same type. Otherwise, it will process recursively.
    pub fn set_value_by_path(&mut self, path: &str, value: Value) -> Result<()> {
        let value_type = get_value_type(&value);
        let self_value_type = get_value_type(&self.value);

        // Check type
        if self_value_type == "object" || self_value_type == "array" {
            if path.is_empty() {
                if self_value_type == value_type {
                    self.value = value.clone();
                } else {
                    return Err(anyhow!("The new value's type is not the same as the old one"));
                }
            }
        } else {
            return Err(anyhow!("The value must be object or array"));
        }

        let mut slice_path: Vec<&str> = path.split('.').collect();
        let val = self.value.clone();

        let k = slice_path[0];

        let (next_val, idx) = match k.parse::<usize>() {
            Ok(n) => {
                // n can be an integer.
                // So if val is an array, val[n] is an element of array
                if get_value_type(&val).as_str() == "array" {
                    (val[n].clone(), n as i32)
                } else { // Otherwise, it is a just an element of a object so string
                    (val[k].clone(), -1)
                }
            },
            Err(_) => (val[k].clone(), -1)
        };

        let new_val_type = get_value_type(&next_val);
        if new_val_type == "null" {
            return Err(anyhow!("{}", format!("Value corresponding to the index {} can not be null", k)));
        }

        // If next_val is simple type: number, string or bool. In this case, we replace immediately
        // the new value if the type matches
        if new_val_type == "number" ||
            new_val_type == "bool" ||
            new_val_type == "string" {
                if new_val_type == value_type {
                    if idx > -1 {
                        self.value[idx as usize] = value;
                    } else {
                        self.value[k] = value;
                    }

                    return Ok(());
                } else {
                    return Err(anyhow!("The new value's type is not the same as the old one"));
                }
            }

        // Otherwise, do recursive
        let mut jo = JsonOps::new(next_val);
        slice_path.remove(0);
        jo.set_value_by_path(slice_path.join(".").as_str(), value)?;

        self.value[k] = jo.value;

        Ok(())
    }

    /// Add a new value to an object or an array
    ///
    /// The current value must be an object or an array. Otherwise, it returns an error.
    /// For an array, the new value will be added at the end of it. For an object, a new field
    /// will be added if it does not exist before. Otherwise, the existing field gets updated.
    pub fn add_value_by_path(&mut self, path: &str, value: Value) -> Result<()> {
        let self_value_type = get_value_type(&self.value);

        // Check type
        if self_value_type != "object" && self_value_type != "array" {
            return Err(anyhow!("The value must be object or array"));
        }

        // If path not specified and current value is an array, we try to push
        if path.is_empty() && self_value_type == "array" {
            if let Some(v) = self.value.as_array_mut() {
                v.push(value);

                return Ok(());
            } else {
                return Err(anyhow!("Cannot convert value to array"));
            }
        }

        let mut slice_path: Vec<&str> = path.split('.').collect();
        let val = self.value.clone();

        let k = slice_path[0];

        let next_val = match k.parse::<usize>() {
            Ok(n) => {
                // n can be an integer.
                // So if val is an array, val[n] is an element of array
                if get_value_type(&val).as_str() == "array" {
                    val[n].clone()
                } else { // Otherwise, it is a just an element of a object so string
                    val[k].clone()
                }
            },
            Err(_) => val[k].clone()
        };

        let new_val_type = get_value_type(&next_val);

        if new_val_type.as_str() == "null" {
                //return Err(anyhow!("{}", format!("Value corresponding to the index {} can not be null", k)));
            if self_value_type == "object" {
                if let Some(o) = self.value.as_object_mut() {
                    if o.insert(k.to_string(), value).is_some() {
                        warn!("Index already exists, its value is updated with the new one");
                    }

                    return Ok(());
                } else {
                    return Err(anyhow!("Cannot convert value to object"));
                }
            }
        }

        if new_val_type == "number" ||
            new_val_type == "bool" ||
            new_val_type == "string" {
                return Err(anyhow!("{}", format!("Cannot add new value to a simple value's type: {}", new_val_type)));
        }

        // Add value if we reach the last element of the path
        if slice_path.len() <= 1 {
            if new_val_type == "array" {
                if let Some(v) = self.value[k].as_array_mut() {
                    v.push(value);

                    return Ok(());
                } else {
                    return Err(anyhow!("Cannot convert value to array"));
                }
            }

            if new_val_type == "object" {
                if let Some(o) = self.value[k].as_object_mut() {
                    if o.insert(k.to_string(), value).is_some() {
                        warn!("Index already exists, its value is updated with the new one");
                    }

                    return Ok(());
                } else {
                    return Err(anyhow!("Cannot convert value to object"));
                }
            }
        }

        // Otherwise, do recursive
        let mut jo = JsonOps::new(next_val);
        slice_path.remove(0);
        jo.add_value_by_path(slice_path.join(".").as_str(), value)?;

        self.value[k] = jo.value;

        Ok(())
    }


    pub fn remove_value_by_path(&mut self, path: &str) -> Result<()> {
        let self_value_type = get_value_type(&self.value);

        // Check type
        if self_value_type != "object" && self_value_type != "array" {
            return Err(anyhow!("The value must be object or array"));
        }

        // If path not specified and current value is an array, we try to push
        if path.is_empty() {
            return Err(anyhow!("The path cannot be empty"));
        }

        let mut slice_path: Vec<&str> = path.split('.').collect();
        let val = self.value.clone();

        let k = slice_path[0];

        let (next_val, idx) = match k.parse::<usize>() {
            Ok(n) => {
                // n can be an integer.
                // So if val is an array, val[n] is an element of array
                if get_value_type(&val).as_str() == "array" {
                    (val[n].clone(), n as i32)
                } else { // Otherwise, it is a just an element of a object so string
                    (val[k].clone(), -1)
                }
            },
            Err(_) => (val[k].clone(), -1)
        };

        let new_val_type = get_value_type(&next_val);
        if new_val_type == "null" {
            return Err(anyhow!("{}", format!("Cannot remove a null value from index: {}", k)));
        }

        let val_type = get_value_type(&val);
        // Remove value if we reach the last element of the path
        if slice_path.len() <= 1 {
            if val_type == "array" {
                if let Some(v) = self.value.as_array_mut() {
                    v.remove(idx as usize);

                    return Ok(());
                } else {
                    return Err(anyhow!("Cannot convert value to array"));
                }
            }

            if val_type == "object" {
                if let Some(o) = self.value.as_object_mut() {
                    match o.remove(&k.to_string()) {
                        Some(_) => (),
                        None => warn!("No key {} to remove was previously in the object", k),
                    }

                    return Ok(());
                } else {
                    return Err(anyhow!("Cannot convert value to object"));
                }
            }
        }

        // Otherwise, do recursive
        let mut jo = JsonOps::new(next_val);
        slice_path.remove(0);
        jo.remove_value_by_path(slice_path.join(".").as_str())?;

        self.value[k] = jo.value;

        Ok(())
    }
}

fn get_value_type(value: &Value) -> String {
    match value {
        Value::Number(_) => "number".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
        Value::Null => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
    struct Topic {
        name: String,
        event: String
    }

    #[test]
    fn test_get_value() {
        let value = json!({"sources": [{"name": "kafka1", "plugin": "kafka", "params": {"brokers": ["127.0.0.1:9092"], "consumer": {"group_id": "group1", "topics": [{"name": "topic1", "event": "event1"}, {"name": "topic2", "event": "event2"}], "offset": "earliest"}}}]});

        let jop = JsonOps::new(value);

        let offset: String = jop.get_value("sources.0.params.consumer.offset");
        let brokers_fake: Vec<String> = jop.get_value("sources.0.params.broker");
        let brokers: Vec<String> = jop.get_value("sources.0.params.brokers");

        let topics: Topic = jop.get_value("sources.0.params.consumer.topics.1");

        assert_eq!("earliest".to_string(), offset);
        assert_eq!(Vec::<String>::new(), brokers_fake);
        assert_eq!(vec!["127.0.0.1:9092"], brokers);
        assert_eq!(Topic{name: "topic2".to_string(), event: "event2".to_string()}, topics);
    }

    #[test]
    fn test_set_value_by_path() {
        let value: Value = serde_json::from_str(r#"{
                "string": "text",
                "boolean": true,
                "int": 5,
                "float": 12.5,
                "arr_str": ["str1", "str2", "str3"],
                "arr_int": [1, 2, 3],
                "arr_bool": [true, false, true],
                "arr_mixted": [1, true, "str1", 12.5],
                "object": {
                    "field1": "text1",
                    "field2": 3,
                    "field3": [1, 2, 3],
                    "field4": {
                        "field41": "text1"
                    }
                }
            }"#).unwrap();

        let mut jop = JsonOps::new(value);

        jop.set_value_by_path("string", json!("text2")).unwrap();
        jop.set_value_by_path("int", json!(10)).unwrap();
        jop.set_value_by_path("float", json!(20.55)).unwrap();
        jop.set_value_by_path("boolean", json!(false)).unwrap();
        jop.set_value_by_path("arr_str.0", json!("string")).unwrap();
        jop.set_value_by_path("arr_int.0", json!(3)).unwrap();
        jop.set_value_by_path("arr_bool.1", json!(true)).unwrap();
        jop.set_value_by_path("arr_mixted.1", json!(false)).unwrap();
        jop.set_value_by_path("arr_mixted.3", json!(20.55)).unwrap();
        jop.set_value_by_path("object.field3.2", json!(1)).unwrap();
        jop.set_value_by_path("object.field4.field41", json!("str1")).unwrap();

        let expected: Value = serde_json::from_str(r#"{
                "string": "text2",
                "boolean": false,
                "int": 10,
                "float": 20.55,
                "arr_str": ["string", "str2", "str3"],
                "arr_int": [3, 2, 3],
                "arr_bool": [true, true, true],
                "arr_mixted": [1, false, "str1", 20.55],
                "object": {
                    "field1": "text1",
                    "field2": 3,
                    "field3": [1, 2, 1],
                    "field4": {
                        "field41": "str1"
                    }
                }
            }"#).unwrap();

        assert_eq!(expected, jop.value);

        let mut result = jop.set_value_by_path("", json!(1));
        assert_eq!("The new value's type is not the same as the old one",
                    result.err().unwrap().to_string());

        result = jop.set_value_by_path("arr_mixted.2", json!(1));
        assert_eq!("The new value's type is not the same as the old one",
                    result.err().unwrap().to_string());

        result = jop.set_value_by_path("arr_mixted.5", json!(1));
        assert_eq!("Value corresponding to the index 5 can not be null",
                    result.err().unwrap().to_string());
    }

    #[test]
    fn test_add_value_by_path() {
        let value: Value = serde_json::from_str(r#"{
                "string": "text",
                "boolean": true,
                "int": 5,
                "float": 12.5,
                "arr_str": ["str1", "str2", "str3"],
                "arr_int": [1, 2, 3],
                "arr_bool": [true, false, true],
                "arr_mixted": [1, true, "str1", 12.5],
                "object": {
                    "field1": "text1",
                    "field2": 3,
                    "field3": [1, 2, 3],
                    "field4": {
                        "field41": "text1"
                    }
                }
            }"#).unwrap();

        let mut jop = JsonOps::new(value);

        jop.add_value_by_path("arr_str", json!("str4")).unwrap();
        jop.add_value_by_path("arr_int", json!(4)).unwrap();
        jop.add_value_by_path("arr_bool", json!(false)).unwrap();
        jop.add_value_by_path("arr_mixted", json!(false)).unwrap();
        jop.add_value_by_path("object.field5", json!(true)).unwrap();
        jop.add_value_by_path("object.field3", json!(15.55)).unwrap();
        jop.add_value_by_path("object.field4.field42", json!("str1")).unwrap();
        jop.add_value_by_path("object2", json!({"field1": "text1"})).unwrap();

        let expected: Value = serde_json::from_str(r#"{
                "string": "text",
                "boolean": true,
                "int": 5,
                "float": 12.5,
                "arr_str": ["str1", "str2", "str3", "str4"],
                "arr_int": [1, 2, 3, 4],
                "arr_bool": [true, false, true, false],
                "arr_mixted": [1, true, "str1", 12.5, false],
                "object": {
                    "field1": "text1",
                    "field2": 3,
                    "field3": [1, 2, 3, 15.55],
                    "field4": {
                        "field41": "text1",
                        "field42": "str1"
                    },
                    "field5": true
                },
                "object2": {
                    "field1": "text1"
                }
            }"#).unwrap();

        assert_eq!(expected, jop.value);

        let result = jop.add_value_by_path("string", json!("str"));
        assert_eq!("Cannot add new value to a simple value's type: string",
                    result.err().unwrap().to_string());
    }

    #[test]
    fn test_remove_value_by_path() {
        let value: Value = serde_json::from_str(r#"{
                "string": "text",
                "boolean": true,
                "int": 5,
                "float": 12.5,
                "arr_str": ["str1", "str2", "str3"],
                "arr_int": [1, 2, 3],
                "arr_bool": [true, false, true],
                "arr_mixted": [1, true, "str1", 12.5],
                "object": {
                    "field1": "text1",
                    "field2": 3,
                    "field3": [1, 2, 3],
                    "field4": {
                        "field41": "text1"
                    }
                }
            }"#).unwrap();

        let mut jop = JsonOps::new(value);

        jop.remove_value_by_path("string").unwrap();
        jop.remove_value_by_path("int").unwrap();
        jop.remove_value_by_path("arr_str").unwrap();
        jop.remove_value_by_path("arr_bool.1").unwrap();
        jop.remove_value_by_path("arr_mixted.2").unwrap();
        jop.remove_value_by_path("object.field2").unwrap();
        jop.remove_value_by_path("object.field3.0").unwrap();
        jop.remove_value_by_path("object.field4.field41").unwrap();

        let expected: Value = serde_json::from_str(r#"{
                "boolean": true,
                "float": 12.5,
                "arr_int": [1, 2, 3],
                "arr_bool": [true, true],
                "arr_mixted": [1, true, 12.5],
                "object": {
                    "field1": "text1",
                    "field3": [2, 3],
                    "field4": {}
                }
            }"#).unwrap();

        assert_eq!(expected, jop.value);

        let result = jop.remove_value_by_path("string");
        assert_eq!("Cannot remove a null value from index: string",
                    result.err().unwrap().to_string());
    }
}
