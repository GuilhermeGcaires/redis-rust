use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::Config;

#[derive(Debug)]
pub struct Item {
    pub value: String,
    pub expires: Option<Duration>,
    pub created: Instant,
}

impl Item {
    pub fn new(value: String, expires: Option<Duration>) -> Self {
        let time_now = Instant::now();
        Self {
            value,
            expires,
            created: time_now,
        }
    }
    pub fn is_expired(&self) -> bool {
        if let Some(duration) = self.expires {
            self.created.elapsed() >= duration
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct Database {
    pub storage: HashMap<String, Item>,
    pub config: Config,
}

impl Database {
    pub fn new(config: Config) -> Self {
        Self {
            storage: HashMap::new(),
            config,
        }
    }
}
