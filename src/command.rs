use crate::resp::RespType;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        ttl: Option<u64>,
    },
    Get(String),
    ConfigGet(String),
    Keys(String),
    Info,
    Unknown,
    ReplConf(String),
}

impl Command {
    pub fn from_resp(resp: Vec<RespType>) -> Command {
        if let Some(RespType::Array(inner_resp)) = resp.first() {
            if inner_resp.is_empty() {
                return Command::Unknown;
            }

            if let RespType::BulkString(command) = &inner_resp[0] {
                match command.to_lowercase().as_str() {
                    "ping" => Command::Ping,
                    "echo" => {
                        if let Some(RespType::BulkString(arg)) = inner_resp.get(1) {
                            Command::Echo(arg.clone())
                        } else {
                            Command::Unknown
                        }
                    }
                    "set" => {
                        let key = inner_resp
                            .get(1)
                            .and_then(|x| {
                                if let RespType::BulkString(s) = x {
                                    Some(s.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();

                        let value = inner_resp
                            .get(2)
                            .and_then(|x| {
                                if let RespType::BulkString(s) = x {
                                    Some(s.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();

                        let ttl = inner_resp.get(4).and_then(|x| {
                            if let RespType::BulkString(s) = x {
                                s.parse::<u64>().ok()
                            } else {
                                None
                            }
                        });

                        Command::Set { key, value, ttl }
                    }
                    "get" => {
                        if let Some(RespType::BulkString(key)) = inner_resp.get(1) {
                            Command::Get(key.clone())
                        } else {
                            Command::Unknown
                        }
                    }
                    "config" => {
                        if let Some(RespType::BulkString(subcommand)) = inner_resp.get(1) {
                            match subcommand.to_lowercase().as_str() {
                                "get" => {
                                    if let Some(RespType::BulkString(key)) = inner_resp.get(2) {
                                        Command::ConfigGet(key.clone())
                                    } else {
                                        Command::Unknown
                                    }
                                }
                                _ => Command::Unknown,
                            }
                        } else {
                            Command::Unknown
                        }
                    }
                    "keys" => {
                        if let Some(RespType::BulkString(key_value)) = inner_resp.get(0) {
                            Command::Keys(key_value.clone())
                        } else {
                            Command::Unknown
                        }
                    }
                    "info" => Command::Info,
                    "replconf" => {
                        if let Some(RespType::BulkString(subcommand)) = inner_resp.get(1) {
                            match subcommand.to_lowercase().as_str() {
                                "listening-port" => {
                                    if let Some(RespType::BulkString(port)) = inner_resp.get(2) {
                                        Command::ReplConf(format!("listening-port {}", port))
                                    } else {
                                        Command::Unknown
                                    }
                                }
                                "capa" => {
                                    if let Some(RespType::BulkString(capability)) =
                                        inner_resp.get(2)
                                    {
                                        if capability.to_lowercase() == "psync2" {
                                            Command::ReplConf("capa psync2".to_string())
                                        } else {
                                            Command::Unknown
                                        }
                                    } else {
                                        Command::Unknown
                                    }
                                }
                                _ => Command::Unknown,
                            }
                        } else {
                            Command::Unknown
                        }
                    }
                    _ => Command::Unknown,
                }
            } else {
                Command::Unknown
            }
        } else {
            Command::Unknown
        }
    }
}
