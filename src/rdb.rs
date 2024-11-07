use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::{Arc, Mutex};

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
    println!("File path: {:?}", path);

    let file = File::open(path);
    match file {
        Ok(file) => {
            let mut file_buffer: [u8; 1024] = [0; 1024];
            let mut reader = BufReader::new(file);
            reader.read(&mut file_buffer).unwrap();
            println!("Hex Buffer: {:x?}", file_buffer);
            println!("Buffer: {:?}", String::from_utf8_lossy(&file_buffer));
            let mut iterator = file_buffer.iter().skip_while(|&b| *b != 0xfb).skip(1);

            let size_hash_table = *iterator.next().unwrap() as usize;
            let _size_expire_hash_table = iterator.next();
            for _ in 0..size_hash_table {
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
                let value_string = String::from_utf8_lossy(&value_chars).to_string();

                let new_item = Item::new(value_string.clone(), None);

                db.storage.insert(key_string.clone(), new_item);
            }
        }
        Err(_) => eprintln!("Couldn't find rdb file."),
    }
}
