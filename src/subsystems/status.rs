use crate::instance::identifier::UserIdentifier;
use parking_lot::Mutex;
use serde::Serialize;
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use utoipa::ToSchema;

#[derive(Default)]
pub struct SubsystemConnections {
    running: AtomicBool,
    total: AtomicUsize,
    users: Mutex<HashMap<UserIdentifier, usize>>,
    databases: Mutex<HashMap<String, usize>>,
}

impl SubsystemConnections {
    pub fn mark_running(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    pub fn connect(
        self: &Arc<Self>,
        user: UserIdentifier,
        database: Option<String>,
    ) -> ConnectionGuard {
        self.total.fetch_add(1, Ordering::SeqCst);
        increment(&self.users, user);
        if let Some(database) = &database {
            increment(&self.databases, database.clone());
        }

        ConnectionGuard {
            subsystem: Arc::clone(self),
            user,
            database,
        }
    }

    pub fn snapshot(&self) -> SubsystemStatus {
        SubsystemStatus {
            running: self.running.load(Ordering::SeqCst),
            connections: Connections {
                total: self.total.load(Ordering::SeqCst),
                unique_databases: self.databases.lock().len(),
                unique_users: self.users.lock().len(),
            },
        }
    }
}

fn increment<K: Eq + Hash>(map: &Mutex<HashMap<K, usize>>, key: K) {
    *map.lock().entry(key).or_default() += 1;
}

fn decrement<K: Eq + Hash>(map: &Mutex<HashMap<K, usize>>, key: &K) {
    let mut map = map.lock();
    if let Some(count) = map.get_mut(key) {
        *count -= 1;
        if *count == 0 {
            map.remove(key);
        }
    }
}

pub struct ConnectionGuard {
    subsystem: Arc<SubsystemConnections>,
    user: UserIdentifier,
    database: Option<String>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.subsystem.total.fetch_sub(1, Ordering::SeqCst);
        decrement(&self.subsystem.users, &self.user);
        if let Some(database) = &self.database {
            decrement(&self.subsystem.databases, database);
        }
    }
}

#[derive(ToSchema, Serialize)]
pub struct SubsystemStatus {
    pub running: bool,
    pub connections: Connections,
}

#[derive(ToSchema, Serialize)]
pub struct Connections {
    pub total: usize,
    pub unique_databases: usize,
    pub unique_users: usize,
}
