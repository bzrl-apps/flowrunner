use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Message {
    Json(Value),
}
