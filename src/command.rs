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
    Unknown,
}

impl Command {
    pub fn from_resp(resp: Vec<RespType>) -> Command {
        if let Some(RespType::Array(inner_resp)) = resp.get(0) {
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
