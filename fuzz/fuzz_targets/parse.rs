#![no_main]

use libfuzzer_sys::fuzz_target;
use bytes::Bytes;

fuzz_target!(|data: &[u8]| {
    if let Ok(Some((n, frame))) = rusty_redis::resp::parse(data) {
        assert!(n <= data.len());
        let owned = bytes::Bytes::copy_from_slice(&data[..n]);
        let _ = frame.into_value(&owned);      // panics if any span is out of bounds
    }
});
