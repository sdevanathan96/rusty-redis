//! Where `Db` gets the time: the system clock, and a controllable one for
//! tests.

#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_millis() as u64
    }
}

/// One offset, read by both clocks, so a test can never move monotonic time
/// while stream ids stay frozen, or the reverse. The wall clock starts at a
/// plausible time rather than zero, because behavior that depends on how close
/// `mstime()` is to `i64::MAX`, such as the PX overflow in `Db::set`, cannot
/// happen at epoch zero.
#[cfg(test)]
pub struct TestClock {
    start: Instant,
    base_ms: u64,
    offset: Mutex<Duration>,
}

#[cfg(test)]
impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        TestClock {
            start: Instant::now(),
            // A plausible wall clock rather than zero.
            base_ms: 1_700_000_000_000,
            offset: Mutex::new(Duration::ZERO),
        }
    }

    pub fn advance(&self, d: Duration) {
        *self.offset.lock().unwrap() += d;
    }

    /// Kept for callers that think in milliseconds. It advances both readings,
    /// because there is only one now.
    pub fn advance_ms(&self, ms: u64) {
        self.advance(Duration::from_millis(ms));
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.start + *self.offset.lock().unwrap()
    }

    fn now_ms(&self) -> u64 {
        self.base_ms + self.offset.lock().unwrap().as_millis() as u64
    }
}
