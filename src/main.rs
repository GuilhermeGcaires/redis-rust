use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
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
                    .filter(|&&ch| ch != 0 as u8)
                    .copied()
                    .collect::<Vec<u8>>();

                let data = String::from_utf8(filtered_buffer).expect("Expected utf-8 string");
                let command = parse_message(data);

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
