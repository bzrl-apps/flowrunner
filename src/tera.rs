use std::collections::HashMap;

use serde_json::value::Value;

use tera::Result;
use uuid::Uuid;

pub fn generate_uuid(_args: &HashMap<String, Value>) -> Result<Value> {
    let uuid = Uuid::new_v4();

    Ok(Value::String(uuid.to_hyphenated().to_string()))
}
