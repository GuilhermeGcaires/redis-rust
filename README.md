# Redis-Rust Implementation

A Redis server implementation written in Rust. This project implements core Redis functionality including basic commands, persistence, and replication.

## Features

### Basic Redis Commands
- `PING` - Test server connectivity
- `ECHO` - Echo back a message
- `SET` - Set a key-value pair with optional TTL
- `GET` - Retrieve a value by key
- `KEYS` - List all keys in the database
- `CONFIG GET` - Get configuration values
- `INFO` - Get server information

### Data Persistence
- RDB file loading and parsing
- Support for key expiration

### Replication
- Master-Slave replication
- PSYNC command implementation
- Replica state propagation

### RESP Protocol
- Redis Serialization Protocol implementation
- Command parsing and serialization

## Getting Started

### Prerequisites
- Rust and Cargo (latest stable version)

### Running the Server

```bash
# Run as a standalone server on default port 6379
cargo run

# Run with custom port
cargo run -- --port 6380

# Run with persistence enabled
cargo run -- --dir /path/to/dir --dbfilename dump.rdb

# Run as a replica of another Redis server
cargo run -- --replicaof 127.0.0.1:6379
```

### Command-line Arguments

- `--port <PORT>` - TCP port to listen on (default: 6379)
- `--dir <DIR>` - Directory for RDB file storage
- `--dbfilename <FILENAME>` - Name of the RDB file
- `--replicaof <HOST:PORT>` - Connect to specified Redis server as replica

## Architecture

The project is organized into several modules:

- `main.rs` - Server initialization and client handling
- `command.rs` - Redis command implementations
- `database.rs` - In-memory database implementation
- `rdb.rs` - RDB file parsing and loading
- `replication.rs` - Master-slave replication logic
- `resp.rs` - Redis protocol parsing and serialization

## Connecting to the Server

You can connect to this Redis implementation using any standard Redis client:

```bash
redis-cli -p 6379
```

## Limitations

- Limited subset of Redis commands implemented
- Basic replication support
- Simple RDB file parsing


