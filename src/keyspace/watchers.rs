//! WATCH: which connections watch which keys, and whether each one's next EXEC
//! must abort.

use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;

use crate::db::Db;

/// Every open WATCH, indexed both ways, as Redis keeps `db->watched_keys` and
/// `c->watched_keys`. The two maps always mirror each other: conn 3 is in
/// `by_key[k]` exactly when `by_conn[3]` lists `k`. The fields are private so
/// nothing outside these methods can update one without the other.
#[derive(Default)]
pub(super) struct Watches {
    /// Which connections watch each key.
    by_key: HashMap<Bytes, Vec<u64>>,
    /// What each connection watches.
    by_conn: HashMap<u64, Watching>,
}

/// One connection's watches.
#[derive(Default)]
struct Watching {
    /// Each key, with the deadline it had at WATCH: `None` if it was absent or
    /// had no TTL.
    keys: Vec<(Bytes, Option<Instant>)>,
    /// A watched key has changed since WATCH (Redis's `CLIENT_DIRTY_CAS`).
    dirty: bool,
}

impl Watching {
    /// A key that was live at WATCH has passed its deadline since.
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
    /// Watching a key twice keeps the first record, as Redis does.
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

    /// `key` has changed: every connection watching it will abort.
    pub(super) fn touch(&mut self, key: &Bytes) {
        if let Some(conns) = self.by_key.get(key) {
            for conn in conns {
                if let Some(watching) = self.by_conn.get_mut(conn) {
                    watching.dirty = true;
                }
            }
        }
    }

    /// Whether `conn`'s EXEC must abort: a watched key changed, or one that
    /// was live at WATCH has expired since, whether or not anything has
    /// deleted it yet.
    pub(super) fn aborts(&self, conn: u64, db: &Db) -> bool {
        let Some(watching) = self.by_conn.get(&conn) else {
            return false; // watches nothing
        };
        watching.dirty || watching.expired(db)
    }

    /// Drops every watch `conn` holds, and any key nobody watches any more.
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
