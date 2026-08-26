#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    assert!(data != b"redis", "canary fired");
    let _ = rusty_redis::resp::parse(data);
});
