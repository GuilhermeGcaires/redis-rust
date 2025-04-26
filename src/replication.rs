use std::sync::{Arc, Mutex};

use anyhow::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{database::Database, handle_client, resp::RespType, Args, Config};

pub async fn handle_replica(
    in_memory: &mut Arc<Mutex<Database>>,
    config: Arc<Config>,
    args: &Args,
) -> Result<(), Error> {
    let host = args
        .replicaof
        .clone()
        .expect("Expected host and port to be passed")
        .replace(" ", ":");

    let mut stream = TcpStream::connect(&host).await?;

    send_ping(&mut stream).await?;
    println!("Received PONG from master");

    send_replconf_listening_port(&mut stream, config.port).await?;
    println!("Master acknowledged REPLCONF listening-port");

    send_replconf_capa_psync2(&mut stream).await?;
    println!("Master acknowledged REPLCONF capa psync2");

    send_psync(&mut stream, &config.repl_id).await?;
    println!("Replication handshake completed successfully!");

    let mut in_memory_cloned = Arc::clone(&in_memory);
    let config_cloned = Arc::clone(&config);

    tokio::spawn(async move {
        println!("Current config: {:?}", config_cloned.clone());
        handle_client(stream, &mut in_memory_cloned, config_cloned).await;
    });
    Ok(())
}

async fn send_ping(stream: &mut TcpStream) -> Result<(), Error> {
    let ping = RespType::Array(vec![RespType::BulkString("PING".to_string())]).serialize();
    stream.write_all(ping.as_bytes()).await?;
    stream.flush().await?;

    let mut buff = vec![0; 1024];
    let bytes_read = stream.read(&mut buff).await?;
    let response = String::from_utf8_lossy(&buff[..bytes_read]);
    if !response.starts_with("+PONG") {
        return Err(Error::msg("Master didn't respond with PONG to PING"));
    }
    Ok(())
}

async fn send_replconf_listening_port(stream: &mut TcpStream, port: u32) -> Result<(), Error> {
    let repl_conf_port = RespType::Array(vec![
        RespType::BulkString("REPLCONF".to_string()),
        RespType::BulkString("listening-port".to_string()),
        RespType::BulkString(port.to_string()),
    ])
    .serialize();

    stream.write_all(repl_conf_port.as_bytes()).await?;
    stream.flush().await?;

    let mut buff = vec![0; 1024];
    let bytes_read = stream.read(&mut buff).await?;
    let response = String::from_utf8_lossy(&buff[..bytes_read]);
    println!("REPL CONF Response: {:?}", response);
    if !response.starts_with("+OK") {
        return Err(Error::msg(
            "Master didn't acknowledge REPLCONF listening-port",
        ));
    }
    Ok(())
}

async fn send_replconf_capa_psync2(stream: &mut TcpStream) -> Result<(), Error> {
    let repl_conf_capa = RespType::Array(vec![
        RespType::BulkString("REPLCONF".to_string()),
        RespType::BulkString("capa".to_string()),
        RespType::BulkString("psync2".to_string()),
    ])
    .serialize();

    stream.write_all(repl_conf_capa.as_bytes()).await?;
    stream.flush().await?;

    let mut buff = vec![0; 1024];
    let bytes_read = stream.read(&mut buff).await?;
    let response = String::from_utf8_lossy(&buff[..bytes_read]);
    println!("REPL CONF PYNC Response: {:?}", response);
    if !response.starts_with("+OK") {
        return Err(Error::msg("Master didn't acknowledge REPLCONF capa psync2"));
    }
    Ok(())
}

async fn send_psync(stream: &mut TcpStream, repl_id: &str) -> Result<(), Error> {
    let psync = RespType::Array(vec![
        RespType::BulkString("PSYNC".to_string()),
        RespType::BulkString("?".to_string()),
        RespType::BulkString("-1".to_string()),
    ])
    .serialize();

    stream.write_all(psync.as_bytes()).await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);

    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if !line.starts_with("+FULLRESYNC") {
        return Err(Error::msg(format!("Unexpected response: {}", line)));
    }

    line.clear();
    reader.read_line(&mut line).await?;

    let size = line
        .trim_start_matches('$')
        .trim()
        .parse::<usize>()
        .map_err(|_| Error::msg("Invalid RDB size"))?;

    let mut rdb_data = vec![0u8; size];
    reader.read_exact(&mut rdb_data).await?;

    println!(
        "First few RDB bytes: {:?}",
        &rdb_data[..rdb_data.len().min(32)]
    );

    Ok(())
}
