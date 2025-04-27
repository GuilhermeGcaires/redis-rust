use crate::command::Command;

#[derive(Debug)]
pub enum RespType {
    SimpleString(String),
    BulkString(String),
    Rdb(String),
    NullBulkString,
    Array(Vec<RespType>),
}

impl RespType {
    pub fn serialize(self) -> String {
        match self {
            RespType::SimpleString(s) => format!("+{}\r\n", s),
            RespType::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            RespType::Rdb(s) => s,
            RespType::NullBulkString => "$-1\r\n".to_string(),
            RespType::Array(items) => {
                let mut serialized = format!("*{}\r\n", items.len());
                for item in items {
                    serialized.push_str(&item.serialize());
                }
                serialized
            }
        }
    }
}

pub fn parse_messages(buffer: &str) -> Vec<Command> {
    let mut commands = Vec::new();
    let mut remaining = buffer;

    while !remaining.trim().is_empty() {
        match parse_single_message(remaining) {
            Ok((command, rest)) => {
                commands.push(command);
                remaining = rest;
            }
            Err(_) => break,
        }
    }

    commands
}

fn parse_single_message(buffer: &str) -> Result<(Command, &str), &'static str> {
    let mut lines = buffer.split("\r\n").peekable();
    let mut bytes_consumed = 0;

    let first_line = lines.next().ok_or("Empty buffer")?;
    bytes_consumed += first_line.len() + 2;

    if !first_line.starts_with('*') {
        return Err("Expected array");
    }
    let count = first_line[1..]
        .parse::<usize>()
        .map_err(|_| "Invalid array length")?;

    let mut elements = Vec::with_capacity(count);

    for _ in 0..count {
        let size_line = lines.next().ok_or("Unexpected end of input (size line)")?;
        bytes_consumed += size_line.len() + 2;

        if !size_line.starts_with('$') {
            return Err("Expected bulk string");
        }

        let len = size_line[1..]
            .parse::<usize>()
            .map_err(|_| "Invalid bulk string length")?;

        let data_line = lines.next().ok_or("Unexpected end of input (data line)")?;
        bytes_consumed += data_line.len() + 2;

        if data_line.len() != len {
            return Err("Bulk string length mismatch");
        }

        elements.push(RespType::BulkString(data_line.to_string()));
    }

    let command = Command::from_resp(vec![RespType::Array(elements)]);
    Ok((command, &buffer[bytes_consumed..]))
}
