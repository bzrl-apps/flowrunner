mod config;

#[macro_use]
pub mod plugin;
pub mod message;
mod utils;
mod flow;
mod job;
mod source;

#[macro_export]
macro_rules! plugin_exec_result {
    ($status:expr, $err:expr, $( $key:expr => $val:expr), *) => {
        {
            let mut result = PluginExecResult {
                status: $status,
                error: $err.to_string(),
                output: serde_json::Map::new()
            };

            $( result.output.insert($key.to_string(), $val); )*
            result
        }
    }
}

#[macro_export]
macro_rules! return_plugin_exec_result_err {
    ($result:expr, $err:expr) => {
        {
            $result.status = Status::Ko;
            $result.error = $err;

            return $result;
        }
    }
}

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
