use std::{
    fs::File,
    io::{BufReader, Read},
    sync::{Arc, Mutex},
    time::Duration,
    usize,
};

use clap::Parser;

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
mod resp;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    dir: Option<String>,

    #[arg(long)]
    dbfilename: Option<String>,
}

#[derive(Debug, Default)]
struct Config {
    dir: Option<String>,
    dbfilename: Option<String>,
}

impl Config {
    fn new(dir: Option<String>, dbfilename: Option<String>) -> Self {
        Self { dir, dbfilename }
    }
}

async fn handle_client(mut stream: TcpStream, in_memory: &mut Arc<Mutex<Database>>) {
    println!("Connection created successfully");

    loop {
        let mut buffer = [0; 1024];
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
                        let mut db = in_memory.lock().unwrap();
                        let mut path = db.config.dir.clone().unwrap();
                        let file_name = db.config.dbfilename.clone().unwrap();

                        path.push_str("/");
                        path.push_str(&file_name);
                        println!("File path: {:?}", path);

                        let file = File::open(path);
                        match file {
                            Ok(file) => {
                                let mut file_buffer: [u8; 1024] = [0; 1024];
                                let mut reader = BufReader::new(file);
                                reader.read(&mut file_buffer).unwrap();
                                println!("Hex Buffer: {:x?}", file_buffer);
                                println!("Buffer: {:?}", String::from_utf8_lossy(&file_buffer));
                                let mut iterator =
                                    file_buffer.iter().skip_while(|&b| *b != 0xfb).skip(1);

                                let _size_hash_table = iterator.next();
                                let _size_expire_hash_table = iterator.next();
                                let _value_type = iterator.next();
                                let key_len = *iterator.next().unwrap() as usize;
                                let mut key_chars = Vec::with_capacity(key_len);
                                for _ in 0..key_len {
                                    if let Some(&byte) = iterator.next() {
                                        key_chars.push(byte)
                                    } else {
                                        break;
                                    }
                                }

                                let value_len = *iterator.next().unwrap() as usize;
                                let mut value_chars = Vec::with_capacity(value_len);
                                for _ in 0..value_len {
                                    if let Some(&byte) = iterator.next() {
                                        value_chars.push(byte)
                                    } else {
                                        break;
                                    }
                                }

                                let key_string = String::from_utf8_lossy(&key_chars).to_string();
                                let value_string =
                                    String::from_utf8_lossy(&value_chars).to_string();

                                let new_item = Item::new(value_string.clone(), None);

                                db.storage.insert(key_string.clone(), new_item);

                                println!("{in_memory:?}");

                                let response =
                                    RespType::Array(vec![RespType::BulkString(key_string)])
                                        .serialize();
                                println!("{response:?}");

                                response
                            }
                            Err(_) => {
                                println!("Falhou");
                                RespType::Array(vec![]).serialize()
                            }
                        }
                    }
                    Command::Unknown => {
                        RespType::SimpleString("-ERR Unknown command\r\n".to_string()).serialize()
                    }
                };

                // Send the response back to the client
                if let Err(e) = stream.write_all(response.as_bytes()).await {
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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config: Config = match (&args.dir, &args.dbfilename) {
        (Some(dir), Some(db_filename)) => Config::new(Some(dir.clone()), Some(db_filename.clone())),
        (Some(_), None) | (None, Some(_)) => {
            eprintln!("Error: Both --dir and --dbfilename must be provided together.");
            std::process::exit(1);
        }
        (None, None) => Config::new(None, None),
    };

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let in_memory: Arc<Mutex<Database>> = Arc::new(Mutex::new(Database::new(config)));
    println!("{:?}", in_memory);

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
