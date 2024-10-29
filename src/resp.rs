use std::iter::Peekable;
use std::str::Lines;

use crate::command::Command;

#[derive(Debug)]
pub enum RespType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<RespType>),
}

impl RespType {
    pub fn get_command(&self) -> Option<(String, Vec<String>)> {
        if let RespType::Array(values) = self {
            if let RespType::BulkString(command) = values.get(0).expect("Couldnt get command") {
                let args = values
                    .iter()
                    .skip(1)
                    .filter_map(|arg| {
                        if let RespType::BulkString(arg_str) = arg {
                            Some(arg_str.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                return Some((command.to_string().to_lowercase(), args));
            }
        }
        None
    }
    pub fn serialize(self) -> String {
        match self {
            RespType::SimpleString(s) => format!("+{}\r\n", s),
            RespType::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            RespType::NullBulkString => format!("$-1\r\n"),
            _ => panic!("Unsupported data type"),
        }
    }
}

pub fn parse_message(buffer: String) -> Command {
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
