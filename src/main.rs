use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Error;
use clap::Parser;

use rdb::load_rdb_to_database;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::resp::{parse_message, RespType};
use crate::{
    command::Command,
    database::{Database, Item},
};

mod command;
mod database;
mod rdb;
mod resp;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    dir: Option<String>,

    #[arg(long)]
    dbfilename: Option<String>,

    #[arg(long)]
    port: Option<u32>,

    #[arg(long)]
    replicaof: Option<String>,
}
#[derive(Debug, PartialEq, Eq)]
enum Role {
    Slave,
    Master,
}

#[derive(Debug)]
struct Config {
    dir: Option<String>,
    dbfilename: Option<String>,
    role: Role,
    port: u32,
    replicaof: Option<String>,
}

impl Config {
    fn new(
        dir: Option<String>,
        dbfilename: Option<String>,
        role: Role,
        port: u32,
        replicaof: Option<String>,
    ) -> Self {
        Self {
            dir,
            dbfilename,
            role,
            port,
            replicaof,
        }
    }
}

async fn handle_client(mut stream: TcpStream, in_memory: &mut Arc<Mutex<Database>>) {
    println!("Connection created successfully");

    loop {
        let mut buffer = [0; 1024];
        println!("buffer: {buffer:?}");
        match stream.read(&mut buffer).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("The connection has been closed");
                    break;
                }

                let filtered_buffer = buffer
                    .iter()
                    .filter(|&&ch| ch != 0_u8)
                    .copied()
                    .collect::<Vec<u8>>();

                let data = String::from_utf8(filtered_buffer).expect("Expected utf-8 string");
                println!("Buffer= {:?}", data);
                let command = parse_message(data);
                println!("{command:?}");

                let response = match command {
                    Command::Ping => RespType::SimpleString("PONG".to_string()).serialize(),
                    Command::Echo(msg) => RespType::BulkString(msg).serialize(),
                    Command::Set { key, value, ttl } => {
                        let item = Item::new(value, ttl.map(Duration::from_millis));
                        in_memory
                            .lock()
                            .expect("Could not lock in_memory db")
                            .storage
                            .insert(key, item);
                        RespType::SimpleString("OK".to_string()).serialize()
                    }
                    Command::Get(key) => match in_memory.lock().unwrap().storage.get(&key) {
                        Some(item) => {
                            println!("Testando get");
                            if item.is_expired() {
                                RespType::NullBulkString.serialize()
                            } else {
                                RespType::SimpleString(item.value.to_string()).serialize()
                            }
                        }
                        None => RespType::NullBulkString.serialize(),
                    },
                    Command::ConfigGet(key) => match key.as_str() {
                        "dir" => {
                            let db = in_memory.lock().expect("Couldn't lock db");
                            if let Some(dir_val) = &db.config.dir {
                                RespType::Array(vec![
                                    RespType::BulkString("dir".to_string()),
                                    RespType::BulkString(dir_val.to_string()),
                                ])
                                .serialize()
                            } else {
                                RespType::NullBulkString.serialize()
                            }
                        }
                        "dbfilename" => {
                            let db = in_memory.lock().expect("Couldn't lock db");
                            if let Some(dbfilename_val) = &db.config.dbfilename {
                                RespType::Array(vec![
                                    RespType::BulkString("dir".to_string()),
                                    RespType::BulkString(dbfilename_val.to_string()),
                                ])
                                .serialize()
                            } else {
                                RespType::NullBulkString.serialize()
                            }
                        }
                        _ => unimplemented!(),
                    },
                    Command::Keys(_) => {
                        let db = in_memory.lock().unwrap();
                        let db_keys = db
                            .storage
                            .keys()
                            .map(|key| RespType::BulkString(key.clone())) // Convert each `&String` to `RespType::BulkString`
                            .collect::<Vec<RespType>>();

                        println!("DB keys: {db_keys:?}");

                        RespType::Array(db_keys).serialize()
                    }
                    Command::Info => {
                        let mut response: String = String::new();
                        let db = in_memory.lock().unwrap();
                        let role = &db.config.role;

                        if *role == Role::Master {
                            response.push_str("role:master\n");
                        } else {
                            response.push_str("role:slave\n");
                        }
                        response.push_str("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0");
                        RespType::BulkString(response).serialize()
                    }
                    Command::ReplConf(arg) => {
                        println!("{arg:?}");
                        RespType::SimpleString("OK".to_string()).serialize()
                        //if arg.starts_with("listening-port") {
                        //    RespType::SimpleString("+OK\r\n".to_string()).serialize()
                        //} else if arg == "capa psync2" {
                        //    RespType::SimpleString("+OK\r\n".to_string()).serialize()
                        //} else {
                        //    RespType::SimpleString("-ERR Unknown REPLCONF argument\r\n".to_string())
                        //        .serialize()
                        //}
                    }
                    Command::PSync => RespType::SimpleString("OK\r\n".to_string()).serialize(),
                    Command::Unknown => {
                        RespType::SimpleString("-ERR Unknown command\r\n".to_string()).serialize()
                    }
                };

                if let Err(e) = stream.write_all(response.as_bytes()).await {
                    stream.flush().await.expect("Flush failed");
                    eprintln!("Error sending response: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error reading stream: {}", e);
                break;
            }
        }
    }
}

async fn handle_replica(config: &Config, args: &Args) -> Result<(), Error> {
    let host = args
        .replicaof
        .clone()
        .expect("Expected host and port to be passed")
        .replace(" ", ":");

    let mut stream = TcpStream::connect(&host)
        .await
        .expect("Coudln't create TCPStream");
    let ping = RespType::Array(vec![RespType::BulkString("PING".to_string())]).serialize();
    stream.write_all(ping.as_bytes()).await?;
    stream.flush().await?;

    let mut buff = vec![0; 1024];
    let bytes_read = stream.read(&mut buff).await?;
    let response = String::from_utf8_lossy(&buff[..bytes_read]);
    if !response.starts_with("+PONG") {
        return Err(Error::msg("Master didn't respond with PONG to PING"));
    }

    println!("Received PONG from master");

    let repl_conf_port = RespType::Array(vec![
        RespType::BulkString("REPLCONF".to_string()),
        RespType::BulkString("listening-port".to_string()),
        RespType::BulkString(config.port.to_string()),
    ])
    .serialize();

    stream.write_all(repl_conf_port.as_bytes()).await?;
    stream.flush().await?;

    let bytes_read = stream.read(&mut buff).await?;
    let response = String::from_utf8_lossy(&buff[..bytes_read]);
    println!("{:?}", response);
    if !response.starts_with("+OK") {
        return Err(Error::msg(
            "Master didn't acknowledge REPLCONF listening-port",
        ));
    }
    println!("Master acknowledged REPLCONF listening-port");

    let repl_conf_capa = RespType::Array(vec![
        RespType::BulkString("REPLCONF".to_string()),
        RespType::BulkString("capa".to_string()),
        RespType::BulkString("psync2".to_string()),
    ])
    .serialize();

    stream.write_all(repl_conf_capa.as_bytes()).await?;
    stream.flush().await?;

    let bytes_read = stream.read(&mut buff).await?;
    let response = String::from_utf8_lossy(&buff[..bytes_read]);
    if !response.starts_with("+OK") {
        return Err(Error::msg("Master didn't acknowledge REPLCONF capa psync2"));
    }
    println!("Master acknowledged REPLCONF capa psync2");
    println!("Replication handshake completed successfully!");

    let psync = RespType::Array(vec![
        RespType::BulkString("PSYNC".to_string()),
        RespType::BulkString("?".to_string()),
        RespType::BulkString("-1".to_string()),
    ])
    .serialize();

    stream.write_all(psync.as_bytes()).await?;
    stream.flush().await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let role = if args.replicaof.is_some() {
        Role::Slave
    } else {
        Role::Master
    };

    let port = args.port.unwrap_or(6379);

    let config: Config = match (&args.dir, &args.dbfilename) {
        (Some(dir), Some(db_filename)) => Config::new(
            Some(dir.clone()),
            Some(db_filename.clone()),
            role,
            port,
            args.replicaof.clone(),
        ),
        (Some(_), None) | (None, Some(_)) => {
            eprintln!("Error: Both --dir and --dbfilename must be provided together.");
            std::process::exit(1);
        }
        (None, None) => Config::new(None, None, role, port, args.replicaof.clone()),
    };

    if config.role == Role::Slave {
        if let Err(e) = handle_replica(&config, &args).await {
            eprintln!("Failed to establish replication connection: {}", e);
            std::process::exit(1);
        }
    }

    let in_memory: Arc<Mutex<Database>> = Arc::new(Mutex::new(Database::new(config)));
    load_rdb_to_database(Arc::clone(&in_memory));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let mut in_memory_cloned = Arc::clone(&in_memory);
                tokio::spawn(async move { handle_client(stream, &mut in_memory_cloned).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        };
    }
}
