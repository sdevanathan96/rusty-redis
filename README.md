# rusty-redis

A Redis-compatible server written in Rust on top of Tokio. It speaks RESP, so
`redis-cli` and ordinary Redis client libraries can talk to it.

## Supported commands

| Group   | Commands                                                        |
|---------|-----------------------------------------------------------------|
| Generic | `PING`, `ECHO`, `DEL`, `EXISTS`, `TYPE`                         |
| Strings | `GET`, `SET` (with `EX` / `PX` expiry)                          |
| Lists   | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LMOVE`, `BLPOP`, `BRPOP`, `BLMOVE` |
| Streams | `XADD`, `XRANGE`, `XREAD` (with `COUNT` / `BLOCK`), `XLEN`, `XDEL` |

Error messages follow real Redis byte for byte, including its quirks.

## Running

Requires Rust 1.85+ (edition 2024).

```sh
./run.sh            # or: cargo run --release
redis-cli -p 6379 ping
```

The server listens on `127.0.0.1:6379`.

## Testing

```sh
cargo test          # unit tests
./test.sh           # differential tests against a real redis-server
```

`test.sh` runs each scenario against this server (port 6379) and a real
`redis-server` (port 6380) and diffs the replies, so real Redis is the oracle.
It needs `redis-cli`, `redis-server`, `nc` and `python3`. Run `./test.sh list`
to filter by scenario name.

## Fuzzing

The RESP parser has a `cargo-fuzz` target:

```sh
cargo +nightly fuzz run parse
```

## Architecture

```mermaid
flowchart TD

subgraph group_network["TCP and RESP"]
  node_server["TCP server<br/>[main.rs]"]
  node_resp["RESP codec<br/>[resp.rs]"]
end

subgraph group_commands["Command Execution"]
  node_command["Command parsing and dispatch<br/>[command.rs]"]
  node_generic["Generic commands<br/>[generic.rs]"]
  node_strings["String commands<br/>[string.rs]"]
  node_lists["List commands<br/>[list.rs]"]
  node_streams["Stream commands<br/>[stream.rs]"]
end

subgraph group_keyspace["Keyspace Coordination"]
  node_keyspace_task["Keyspace owner and blocking coordination<br/>[keyspace.rs]"]
end

subgraph group_storage["In-Memory Storage"]
  node_db[("In-memory database<br/>[db.rs]")]
  node_stream_model["Stream entries and IDs<br/>[stream.rs]"]
end

node_client(("Redis client"))

node_client -->|"sends requests"| node_server
node_server -->|"parses frames"| node_resp
node_server -->|"parses commands"| node_command
node_server -->|"submits requests"| node_keyspace_task
node_keyspace_task -->|"executes commands"| node_command
node_command -->|"dispatches"| node_generic
node_command -->|"dispatches"| node_strings
node_command -->|"dispatches"| node_lists
node_command -->|"dispatches"| node_streams
node_generic -->|"reads and writes"| node_db
node_strings -->|"reads and writes"| node_db
node_lists -->|"reads and writes"| node_db
node_streams -->|"reads and writes"| node_db
node_db -->|"uses stream model"| node_stream_model
node_server -->|"encodes replies"| node_resp
node_keyspace_task -->|"returns replies"| node_server
node_server -->|"sends replies"| node_client

click node_server "https://github.com/sdevanathan96/rusty-redis/blob/main/src/main.rs"
click node_resp "https://github.com/sdevanathan96/rusty-redis/blob/main/src/resp.rs"
click node_command "https://github.com/sdevanathan96/rusty-redis/blob/main/src/command.rs"
click node_generic "https://github.com/sdevanathan96/rusty-redis/blob/main/src/command/generic.rs"
click node_strings "https://github.com/sdevanathan96/rusty-redis/blob/main/src/command/string.rs"
click node_lists "https://github.com/sdevanathan96/rusty-redis/blob/main/src/command/list.rs"
click node_streams "https://github.com/sdevanathan96/rusty-redis/blob/main/src/command/stream.rs"
click node_keyspace_task "https://github.com/sdevanathan96/rusty-redis/blob/main/src/keyspace.rs"
click node_db "https://github.com/sdevanathan96/rusty-redis/blob/main/src/db.rs"
click node_stream_model "https://github.com/sdevanathan96/rusty-redis/blob/main/src/db/stream.rs"

classDef toneNeutral fill:#f8fafc,stroke:#334155,stroke-width:1.5px,color:#0f172a
classDef toneBlue fill:#dbeafe,stroke:#2563eb,stroke-width:1.5px,color:#172554
classDef toneAmber fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#78350f
classDef toneMint fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
classDef toneRose fill:#ffe4e6,stroke:#e11d48,stroke-width:1.5px,color:#881337
classDef toneIndigo fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#312e81
classDef toneTeal fill:#ccfbf1,stroke:#0f766e,stroke-width:1.5px,color:#134e4a
class node_server,node_resp toneBlue
class node_command,node_generic,node_strings,node_lists,node_streams,node_client toneAmber
class node_keyspace_task toneMint
class node_db,node_stream_model toneRose
```

## Layout

```
src/
  main.rs        TCP accept loop and per-connection handling
  resp.rs        RESP parser and encoder
  command.rs     request -> Command parsing, error replies
  command/       per-group parsers (generic, string, list, stream)
  db.rs          in-memory data store and expiry
  db/stream.rs   stream entries and IDs
  keyspace.rs    single task owning the keyspace; blocking-command wakeups
fuzz/            cargo-fuzz targets
```

## Credits

- [redis-oxide](https://github.com/dpbriggs/redis-oxide), for the shape of the
  RESP parser: it first records byte offsets into the input and resolves them
  into values afterwards and each parse step returns
  `Result<Option<(usize, T)>, E>`.
- [Redis](https://github.com/redis/redis), the reference for every behavior and
  error message here and the server `test.sh` diffs against.
