use std::sync::{Arc, Mutex};

use clap::Parser;

use rdb::load_rdb_to_database;
use replication::handle_replica;
use resp::parse_messages;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::command::handle_command;
use crate::{
    command::Command,
    database::{Database, Item},
};

mod command;
mod database;
mod rdb;
mod replication;
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

#[derive(Debug, PartialEq, Eq, Clone)]
enum Role {
    Slave,
    Master,
}

#[derive(Debug, Clone)]
struct Config {
    dir: Option<String>,
    dbfilename: Option<String>,
    role: Role,
    port: u32,
    repl_id: String,
    replicaof: Option<String>,
    replication_manager: ReplicationManager,
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
            repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            replicaof,
            replication_manager: ReplicationManager::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct ReplicationManager {
    replicas: Arc<RwLock<Vec<TcpStream>>>,
}

impl ReplicationManager {
    fn new() -> Self {
        Self {
            replicas: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

async fn handle_client(
    mut stream: TcpStream,
    in_memory: &mut Arc<Mutex<Database>>,
    config: Arc<Config>,
) {
    println!("Connection created successfully");
    let mut last_command = Command::Unknown;

    loop {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("The connection has been closed");
                    break;
                }
                println!("Bytes read: {bytes_read:?}");
                let filtered_buffer = buffer
                    .iter()
                    .take(bytes_read)
                    .filter(|&&ch| ch != 0_u8)
                    .copied()
                    .collect::<Vec<u8>>();

                let data = String::from_utf8(filtered_buffer).expect("Expected utf-8 string");
                let commands = parse_messages(&data);
                println!("{:?}", commands);

                for command in commands {
                    last_command = command.clone();

                    if let Some(response) =
                        handle_command(&command, &mut stream, in_memory, &config).await
                    {
                        if let Err(e) = stream.write_all(response.as_bytes()).await {
                            eprintln!("Error sending response: {}", e);
                            break;
                        }
                    }

                    if command == Command::PSync {
                        println!("Inserting stream onto replication manager.");
                        break;
                    }
                }

                if last_command == Command::PSync {
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error reading stream: {}", e);
                break;
            }
        }
    }

    if last_command == Command::PSync {
        config
            .replication_manager
            .replicas
            .write()
            .await
            .push(stream);
        println!("{config:?}");
    }
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

    let config = Arc::new(config);
    let in_memory: Arc<Mutex<Database>> = Arc::new(Mutex::new(Database::new(Arc::clone(&config))));

    if config.role == Role::Slave && args.replicaof.is_some() {
        let mut in_memory_cloned = Arc::clone(&in_memory);
        let config_cloned = Arc::clone(&config);
        if let Err(e) = handle_replica(&mut in_memory_cloned, config_cloned, &args).await {
            eprintln!("Failed to establish replication connection: {}", e);
            std::process::exit(1);
        }
    }

    load_rdb_to_database(Arc::clone(&in_memory));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    println!("Listening on {:?}", port);

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let mut in_memory_cloned = Arc::clone(&in_memory);
                let config_cloned = Arc::clone(&config);
                tokio::spawn(async move {
                    println!("Current config: {:?}", config_cloned.clone());
                    handle_client(stream, &mut in_memory_cloned, config_cloned).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        };
    }
}
