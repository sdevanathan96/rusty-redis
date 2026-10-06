//! WATCH: who watches which keys, and whose EXEC must abort.

use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;

use crate::db::Db;

/// Every watch, indexed both ways as in Redis. The two maps always mirror each
/// other, which is why the fields are private.
#[derive(Default)]
pub(super) struct Watches {
    by_key: HashMap<Bytes, Vec<u64>>,
    by_conn: HashMap<u64, Watching>,
}

#[derive(Default)]
struct Watching {
    /// Each key, with its deadline at WATCH time.
    keys: Vec<(Bytes, Option<Instant>)>,
    /// A watched key has changed (Redis's `CLIENT_DIRTY_CAS`).
    dirty: bool,
}

impl Watching {
    fn expired(&self, db: &Db) -> bool {
        for (_, deadline) in &self.keys {
            if let Some(d) = deadline
                && db.has_passed(*d)
            {
                return true;
            }
        }
        false
    }
}

impl Watches {
    pub(super) fn watch(&mut self, conn: u64, key: Bytes, deadline: Option<Instant>) {
        let conns = self.by_key.entry(key.clone()).or_default();
        if conns.contains(&conn) {
            return;
        }
        conns.push(conn);
        self.by_conn
            .entry(conn)
            .or_default()
            .keys
            .push((key, deadline));
    }

    pub(super) fn touch(&mut self, key: &Bytes) {
        if let Some(conns) = self.by_key.get(key) {
            for conn in conns {
                if let Some(watching) = self.by_conn.get_mut(conn) {
                    watching.dirty = true;
                }
            }
        }
    }

    /// A watched key changed, or expired since WATCH, deleted yet or not.
    pub(super) fn aborts(&self, conn: u64, db: &Db) -> bool {
        let Some(watching) = self.by_conn.get(&conn) else {
            return false; // watches nothing
        };
        watching.dirty || watching.expired(db)
    }

    pub(super) fn unwatch(&mut self, conn: u64) {
        if let Some(watching) = self.by_conn.remove(&conn) {
            for (key, _) in watching.keys {
                if let Some(conns) = self.by_key.get_mut(&key) {
                    conns.retain(|&c| c != conn);
                    if conns.is_empty() {
                        self.by_key.remove(&key);
                    }
                }
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        debug_assert_eq!(self.by_key.is_empty(), self.by_conn.is_empty());
        self.by_conn.is_empty()
    }
}
