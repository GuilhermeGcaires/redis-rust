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
        println!("Resp vector: {:?}", resp.get(0));

        if let Some(RespType::BulkString(command)) = resp.get(0) {
            match command.to_lowercase().as_str() {
                "ping" => Command::Ping,
                "echo" => {
                    if let Some(RespType::BulkString(arg)) = resp.get(1) {
                        Command::Echo(arg.clone())
                    } else {
                        Command::Unknown
                    }
                }
                "set" => {
                    let key = resp.get(1).and_then(|x| {
                        if let RespType::BulkString(s) = x {
                            Some(s.clone())
                        } else {
                            None
                        }
                    });
                    let value = resp.get(2).and_then(|x| {
                        if let RespType::BulkString(s) = x {
                            Some(s.clone())
                        } else {
                            None
                        }
                    });
                    let ttl = resp.get(3).and_then(|x| {
                        if let RespType::BulkString(s) = x {
                            s.parse::<u64>().ok()
                        } else {
                            None
                        }
                    });

                    Command::Set {
                        key: key.unwrap_or_default(),
                        value: value.unwrap_or_default(),
                        ttl,
                    }
                }
                "get" => {
                    if let Some(RespType::BulkString(key)) = resp.get(1) {
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
    }
}
