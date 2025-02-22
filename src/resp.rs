use std::iter::Peekable;
use std::str::Lines;

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
            RespType::Rdb(s) => format!("{}", s),
            RespType::NullBulkString => ("$-1\r\n").to_string(),
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

pub fn parse_message(buffer: String) -> Command {
    println!("Message to be parsed: {:?}", buffer);
    let mut lines = buffer.lines().peekable();
    let mut resp = Vec::new();

    while let Some(line) = lines.next() {
        if line.starts_with("*") {
            if let Ok(count) = line[1..].parse::<usize>() {
                let mut elements = Vec::with_capacity(count);
                for _ in 0..count {
                    if let Some(element) = parse_element(&mut lines) {
                        elements.push(element)
                    }
                }
                resp.push(RespType::Array(elements));
            }
            continue;
        } else if line.starts_with("$") {
            let length = line
                .replace("$", "")
                .parse::<u32>()
                .expect("Unable to parse bulkstring length");
            if let Some(s) = parse_bulk_string(&mut lines, length) {
                resp.push(RespType::BulkString(s));
            }
            continue;
        }
        resp.push(RespType::SimpleString(line.to_string()));
    }

    Command::from_resp(resp)
}

fn parse_element(lines: &mut Peekable<Lines>) -> Option<RespType> {
    match lines.next() {
        Some(line) => {
            if line.starts_with("$") {
                let length = line
                    .replace("$", "")
                    .parse::<u32>()
                    .expect("Unable to parse bulkstring length");
                if let Some(s) = parse_bulk_string(lines, length) {
                    return Some(RespType::BulkString(s));
                }
            } else if line.starts_with("*") {
                if let Ok(count) = line[1..].parse::<usize>() {
                    let mut elements = Vec::with_capacity(count);
                    for _ in 0..count {
                        if let Some(element) = parse_element(lines) {
                            elements.push(element)
                        }
                    }
                    return Some(RespType::Array(elements));
                }
            } else {
                return Some(RespType::SimpleString(line.to_string()));
            }
        }
        _ => (),
    }
    None
}

fn parse_bulk_string(lines: &mut Peekable<Lines>, length: u32) -> Option<String> {
    if let Some(data) = lines.next() {
        if data.len() as u32 == length {
            return Some(data.to_string());
        } else {
            panic!("String length different than parsed length");
        }
    }
    None
}
