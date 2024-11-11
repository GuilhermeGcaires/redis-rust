use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::Database;
use crate::Item;

pub fn load_rdb_to_database(in_memory: Arc<Mutex<Database>>) {
    let mut db = in_memory.lock().unwrap();

    if db.config.dir == None || db.config.dbfilename == None {
        return;
    }

    let mut path = db
        .config
        .dir
        .clone()
        .expect("Couldn't retrieve path of file");
    let file_name = db.config.dbfilename.clone().unwrap();

    path.push_str("/");
    path.push_str(&file_name);

    let file = File::open(path);
    match file {
        Ok(file) => {
            let mut file_buffer: [u8; 1024] = [0; 1024];
            let mut reader = BufReader::new(file);
            reader.read(&mut file_buffer).unwrap();

            let mut hash_table_size;
            let mut _expire_table_size;
            let mut buffer_iterator = file_buffer.iter();

            while let Some(&control) = buffer_iterator.next() {
                match control {
                    0xFB => {
                        let (table_size, expiry_size) = process_hash_table(&mut buffer_iterator);
                        hash_table_size = table_size;
                        _expire_table_size = expiry_size;
                        println!(
                            "Hash table size: {:?}, Hash expiry size: {:?}",
                            hash_table_size, _expire_table_size
                        );

                        parse_hash_table(&mut buffer_iterator, hash_table_size, &mut db);
                    }
                    _ => {}
                }
            }

            //let size_hash_table = *buffer_iterator.next().unwrap() as usize;
            //let _size_expire_hash_table = buffer_iterator.next();
            //for _ in 0..size_hash_table {
            //    let _value_type = buffer_iterator.next();
            //    let key_len = *buffer_iterator.next().unwrap() as usize;
            //    let mut key_chars = Vec::with_capacity(key_len);
            //    for _ in 0..key_len {
            //        if let Some(&byte) = buffer_iterator.next() {
            //            key_chars.push(byte)
            //        } else {
            //            break;
            //        }
            //    }
            //
            //    let value_len = *buffer_iterator.next().unwrap() as usize;
            //    let mut value_chars = Vec::with_capacity(value_len);
            //    for _ in 0..value_len {
            //        if let Some(&byte) = buffer_iterator.next() {
            //            value_chars.push(byte)
            //        } else {
            //            break;
            //        }
            //    }
            //
            //    let key_string = String::from_utf8_lossy(&key_chars).to_string();
            //    let value_string = String::from_utf8_lossy(&value_chars).to_string();
            //
            //    let new_item = Item::new(value_string.clone(), None);
            //
            //    db.storage.insert(key_string.clone(), new_item);
        }
        Err(_) => eprintln!("Couldn't find rdb file."),
    }
}

fn process_hash_table<'a, I>(buffer_iterator: &mut I) -> (usize, usize)
where
    I: Iterator<Item = &'a u8>,
{
    let size_hash_table = *buffer_iterator.next().unwrap() as usize;
    let size_expire_hash_table = *buffer_iterator.next().unwrap() as usize;

    (size_hash_table, size_expire_hash_table)
}

fn parse_hash_table<'a, I>(buffer_iterator: &mut I, keys_size: usize, db: &mut Database)
where
    I: Iterator<Item = &'a u8>,
{
    for _ in 0..keys_size {
        let value_type = buffer_iterator.next().unwrap();
        let expiry: Option<u128>;
        match value_type {
            0xFC => {
                let expiry_bytes: Vec<u8> = buffer_iterator.take(8).copied().collect();
                println!("{expiry_bytes:2x?}");
                let mut cursor = Cursor::new(expiry_bytes);
                expiry = Some(cursor.read_u64::<LittleEndian>().ok().unwrap() as u128);
                let _ = buffer_iterator.next();
            }
            _ => {
                expiry = None;
            }
        }

        let key_len = *buffer_iterator.next().unwrap() as usize;
        let mut key_chars = Vec::with_capacity(key_len);

        for _ in 0..key_len {
            if let Some(&byte) = buffer_iterator.next() {
                key_chars.push(byte)
            } else {
                break;
            }
        }

        let value_len = *buffer_iterator.next().unwrap() as usize;
        let mut value_chars = Vec::with_capacity(value_len);
        for _ in 0..value_len {
            if let Some(&byte) = buffer_iterator.next() {
                value_chars.push(byte)
            } else {
                break;
            }
        }

        let key_string = String::from_utf8_lossy(&key_chars).to_string();
        let value_string = String::from_utf8_lossy(&value_chars).to_string();

        let new_item = Item::new(value_string.clone(), None);

        if is_expired(expiry) && !expiry.is_none() {
            continue;
        }

        db.storage.insert(key_string.clone(), new_item);
    }
}

fn is_expired(expiry_timestamp_ms: Option<u128>) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let current_timestamp_ms = now.as_millis() as u128;
    expiry_timestamp_ms < Some(current_timestamp_ms)
}
