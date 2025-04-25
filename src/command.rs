use std::sync::{Arc, Mutex};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};

use crate::{
    database::{Database, Item},
    resp::RespType,
    Config, Role,
};

#[derive(Debug, PartialEq, Eq)]
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
    PSync,
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
                    "psync" => Command::PSync,
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

pub async fn handle_command(
    command: &Command,
    stream: &mut TcpStream,
    in_memory: &mut Arc<Mutex<Database>>,
    config: &Arc<Config>,
) -> Option<String> {
    match command {
        Command::Ping => Some(RespType::SimpleString("PONG".to_string()).serialize()),
        Command::Echo(msg) => Some(RespType::BulkString(msg.clone()).serialize()),
        Command::Set { key, value, ttl } => handle_set(key, value, ttl, in_memory, config).await,
        Command::Get(key) => handle_get(key, in_memory),
        Command::ConfigGet(key) => handle_config_get(key, in_memory),
        Command::Keys(_) => handle_keys(in_memory),
        Command::Info => handle_info(in_memory),
        Command::ReplConf(_) => Some(RespType::SimpleString("OK".to_string()).serialize()),
        Command::PSync => {
            handle_psync(stream, config).await;
            None
        }
        Command::Unknown => {
            Some(RespType::SimpleString("-ERR Unknown command".to_string()).serialize())
        }
    }
}

async fn handle_set(
    key: &String,
    value: &String,
    ttl: &Option<u64>,
    in_memory: &mut Arc<Mutex<Database>>,
    config: &Arc<Config>,
) -> Option<String> {
    let item = Item::new(value.clone(), ttl.map(Duration::from_millis));
    in_memory.lock().unwrap().storage.insert(key.clone(), item);

    if config.role == Role::Master {
        let set_command = RespType::Array(vec![
            RespType::BulkString("SET".to_string()),
            RespType::BulkString(key.clone()),
            RespType::BulkString(value.clone()),
        ])
        .serialize();

        for replica in &mut *config.replication_manager.replicas.write().await {
            if let Err(e) = replica.write_all(set_command.as_bytes()).await {
                eprintln!("Error propagating command to replica: {}", e);
            }
        }
    }
    Some(RespType::SimpleString("OK".to_string()).serialize())
}

fn handle_get(key: &String, in_memory: &mut Arc<Mutex<Database>>) -> Option<String> {
    match in_memory.lock().unwrap().storage.get(key) {
        Some(item) if !item.is_expired() => {
            Some(RespType::SimpleString(item.value.to_string()).serialize())
        }
        _ => Some(RespType::NullBulkString.serialize()),
    }
}

fn handle_config_get(key: &String, in_memory: &mut Arc<Mutex<Database>>) -> Option<String> {
    let db = in_memory.lock().unwrap();
    match key.as_str() {
        "dir" => db.config.dir.as_ref().map(|dir| {
            RespType::Array(vec![
                RespType::BulkString("dir".to_string()),
                RespType::BulkString(dir.clone()),
            ])
            .serialize()
        }),
        "dbfilename" => db.config.dbfilename.as_ref().map(|dbfilename| {
            RespType::Array(vec![
                RespType::BulkString("dbfilename".to_string()),
                RespType::BulkString(dbfilename.clone()),
            ])
            .serialize()
        }),
        _ => None,
    }
}

fn handle_keys(in_memory: &mut Arc<Mutex<Database>>) -> Option<String> {
    let db = in_memory.lock().unwrap();
    let db_keys = db
        .storage
        .keys()
        .map(|key| RespType::BulkString(key.clone()))
        .collect::<Vec<RespType>>();
    Some(RespType::Array(db_keys).serialize())
}

fn handle_info(in_memory: &mut Arc<Mutex<Database>>) -> Option<String> {
    let db = in_memory.lock().unwrap();
    let role = if db.config.role == Role::Master {
        "master"
    } else {
        "slave"
    };
    let response = format!(
        "role:{}\nmaster_replid:{}\nmaster_repl_offset:0",
        role, db.config.repl_id
    );
    Some(RespType::BulkString(response).serialize())
}

async fn handle_psync(
    stream: &mut TcpStream,
    config: &Arc<Config>,
) -> Result<(), Box<dyn std::error::Error>> {
    let full_resync =
        RespType::SimpleString(format!("FULLRESYNC {} 0", config.repl_id)).serialize();
    stream.write_all(full_resync.as_bytes()).await?;
    stream.flush().await?;

    let empty_file = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;

    let rdb_header = format!("${}\r\n", empty_file.len());
    stream.write_all(rdb_header.as_bytes()).await?;
    stream.flush().await?;
    stream.write_all(&empty_file).await?;
    stream.flush().await?;

    println!("Successfully sent PSYNC response with RDB file");
    Ok(())
}
