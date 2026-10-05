#!/usr/bin/env bash
#
# Differential test harness for the Redis clone.
#
# Runs each scenario against your server AND a real redis-server, then diffs.
# Real Redis is the oracle, so nothing here hardcodes an expected reply and
# nothing goes stale when your memory of the spec is wrong.
#
#   ./test.sh              run everything
#   ./test.sh list         run only scenarios whose name matches "list"
#   STRICT=1 ./test.sh     also run the known deliberate divergences
#   MINE=6379 REAL=6380 ./test.sh
#
# Requires: redis-cli, redis-server, nc, python3

set -uo pipefail

MINE=${MINE:-6379}
REAL=${REAL:-6380}
FILTER=${1:-}
RUN=$$          # unique per invocation, so keys never collide between runs

PASS=0
FAIL=0
SKIP=0
N=0
FAILED_NAMES=()

RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; DIM=$'\033[2m'; OFF=$'\033[0m'
[ -t 1 ] || { RED=""; GRN=""; YEL=""; DIM=""; OFF=""; }

TIMEOUT_BIN=$(command -v gtimeout || command -v timeout || true)
if [ -z "$TIMEOUT_BIN" ]; then
    echo "need coreutils timeout (brew install coreutils): without it a blocking" >&2
    echo "command stalls the suite instead of failing it" >&2
    exit 1
fi

# ---------------------------------------------------------------- setup

need() { command -v "$1" >/dev/null || { echo "missing: $1"; exit 1; }; }
need redis-cli
need nc
need python3

alive() { redis-cli -p "$1" PING >/dev/null 2>&1; }

if ! alive "$MINE"; then
    echo "${RED}Your server is not answering on port $MINE.${OFF}"
    echo "Start it with:  cargo run"
    exit 1
fi

OUR_REDIS=0
if alive "$REAL"; then
    # Wipe the oracle. Stale keys from a previous run make YOUR server look
    # broken across every scenario that touches the same key names.
    redis-cli -p "$REAL" FLUSHALL >/dev/null
else
    need redis-server
    echo "${DIM}starting reference redis-server on $REAL${OFF}"
    redis-server --port "$REAL" --save '' --appendonly no --daemonize yes >/dev/null
    OUR_REDIS=1
    for _ in $(seq 20); do alive "$REAL" && break; sleep 0.1; done
    alive "$REAL" || { echo "${RED}reference server failed to start${OFF}"; exit 1; }
fi

BLOCK_PY=$(mktemp)

cleanup() {
    rm -f "$BLOCK_PY"
    rm -f "$BLOCK2_PY"
    rm -f "$CONN_PY"
    [ "$OUR_REDIS" = 1 ] && redis-cli -p "$REAL" SHUTDOWN NOSAVE >/dev/null 2>&1
    return 0
}
trap cleanup EXIT

# Driver for two-connection scenarios. Lives in a temp file rather than a
# heredoc inside a function, because nesting quotes that deep is how the
# previous version silently lost half its code.
cat > "$BLOCK_PY" <<'PYEOF'
import socket, sys, threading, time

port      = int(sys.argv[1])
tag       = sys.argv[2]
delay     = float(sys.argv[3])
setup     = sys.argv[4]          # new, "" for none
blocking  = sys.argv[5]
meanwhile = sys.argv[6]

def sub(s):
    return s.replace("{K3}", tag + "c").replace("{K2}", tag + "b").replace("{K}", tag + "a")

def frame(spec):
    """One RESP array from a pipe-separated argument list. Lengths are computed
    from the substituted bytes, so a placeholder can never disagree with its
    length prefix."""
    parts = [sub(p).encode() for p in spec.split("|")]
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out

def frames(spec):
    return b"".join(frame(c) for c in spec.split(";"))

def drain(s, first, quiet):
    """Everything s receives: waits up to `first` seconds for anything at all,
    then stops after `quiet` seconds of silence or when the server closes. Only
    `first` decides whether a missing or late reply is caught; `quiet` just
    ends the wait once the answer is in."""
    buf = b""
    s.settimeout(max(first, 0.05))
    try:
        while True:
            c = s.recv(4096)
            if not c:
                break
            buf += c
            s.settimeout(quiet)
    except socket.timeout:
        pass
    return buf

if setup:
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(frames(setup))
    drain(s, 1.0, 0.2)
    s.close()

blocked = socket.create_connection(("127.0.0.1", port))
blocked.sendall(frame(blocking))
deadline = time.time() + delay + 3.0

def other():
    time.sleep(delay)
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(frames(meanwhile))
    drain(s, 1.0, 0.2)
    s.close()

t = threading.Thread(target=other)
t.start()

# Read only once the other connection is done, so anything its commands cause
# on this one, such as an element wrongly handed over after a timeout, is
# already buffered. The reply itself still gets until the old deadline.
t.join()
buf = drain(blocked, deadline - time.time(), 0.3)
sys.stdout.write(repr(buf))
PYEOF


BLOCK2_PY=$(mktemp)
# add to the cleanup trap: rm -f "$BLOCK2_PY"
cat > "$BLOCK2_PY" <<'PYEOF'
import socket, sys, time

port    = int(sys.argv[1])
tag     = sys.argv[2]
delay   = float(sys.argv[3])
b1, b2  = sys.argv[4], sys.argv[5]
trigger = sys.argv[6]

def sub(s):
    return s.replace("{K3}", tag + "c").replace("{K2}", tag + "b").replace("{K}", tag + "a")

def frame(spec):
    parts = [sub(p).encode() for p in spec.split("|")]
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out

def frames(spec):
    return b"".join(frame(c) for c in spec.split(";"))

def drain(s, first, quiet):
    """Same as in BLOCK_PY: `first` catches a missing or late reply, `quiet`
    only ends the wait once the answer is in."""
    buf = b""
    s.settimeout(max(first, 0.05))
    try:
        while True:
            c = s.recv(4096)
            if not c:
                break
            buf += c
            s.settimeout(quiet)
    except socket.timeout:
        pass
    return buf

def park(spec):
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(frame(spec))
    return s, time.time() + delay + 3.0

s1, d1 = park(b1)
time.sleep(0.3)                 # b1 must park before b2 arrives, for FIFO order
s2, d2 = park(b2)
time.sleep(0.3)

time.sleep(delay)
s = socket.create_connection(("127.0.0.1", port))
s.sendall(frames(trigger))
drain(s, 1.0, 0.2)
s.close()

# Replies wait in each socket's buffer, so reading them after the trigger loses
# nothing, and each still gets until its own deadline.
first = drain(s1, d1 - time.time(), 0.3)
second = drain(s2, d2 - time.time(), 0.3)
sys.stdout.write("first=%r second=%r" % (first, second))
PYEOF

# Driver for scripted multi-connection scenarios. Each argument after the tag
# is one step: "<conn> send <cmds>", "<conn> read", "<conn> close", or
# "sleep <secs>". A connection opens on its first send.
CONN_PY=$(mktemp)
cat > "$CONN_PY" <<'PYEOF'
import socket, sys, time

port  = int(sys.argv[1])
tag   = sys.argv[2]
steps = sys.argv[3:]

def sub(s):
    return s.replace("{K3}", tag + "c").replace("{K2}", tag + "b").replace("{K}", tag + "a")

def frame(spec):
    parts = [sub(p).encode() for p in spec.split("|")]
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out

def frames(spec):
    return b"".join(frame(c) for c in spec.split(";"))

conns = {}
for step in steps:
    words = step.split(" ", 2)
    if words[0] == "sleep":
        time.sleep(float(words[1]))
        continue
    name, action = words[0], words[1]
    if action == "send":
        if name not in conns:
            conns[name] = socket.create_connection(("127.0.0.1", port))
        conns[name].sendall(frames(words[2]))
    elif action == "read":
        # Waits 0.5s for anything, so b'' means nothing arrived in that time,
        # then stops after 0.2s of quiet once something has.
        s = conns[name]
        s.settimeout(0.5)
        buf = b""
        try:
            while True:
                c = s.recv(4096)
                if not c:
                    break
                buf += c
                s.settimeout(0.2)
        except socket.timeout:
            pass
        print("%s: %r" % (name, buf))
    elif action == "close":
        conns.pop(name).close()
PYEOF

# ---------------------------------------------------------------- helpers

skip_filtered() {
    N=$((N + 1))
    if [ -n "$FILTER" ] && [[ "$1" != *"$FILTER"* ]]; then
        SKIP=$((SKIP + 1)); return 0
    fi
    return 1
}

report() {
    local name="$1" kind="$2" a="$3" b="$4" label_a="$5" label_b="$6"
    if [ "$a" = "$b" ] && [ -n "$a" ]; then
        PASS=$((PASS + 1))
        printf '%s  PASS%s  %s%s\n' "$GRN" "$OFF" "$name" "$kind"
    else
        FAIL=$((FAIL + 1)); FAILED_NAMES+=("$name")
        printf '%s  FAIL%s  %s%s\n' "$RED" "$OFF" "$name" "$kind"
        if [ -z "$a" ] && [ -z "$b" ]; then
            printf '        both sides produced NO OUTPUT (harness bug, not a diff)\n'
        else
            diff --label "$label_a" --label "$label_b" -u \
                 <(printf '%s' "$a") <(printf '%s' "$b") | sed 's/^/        /'
        fi
    fi
}

# Runs a command sequence against one port. {K}, {K2}, {K3} become per-scenario
# unique key names. "SLEEP n" is a pseudo command.
run_seq() {
    local port="$1" tag="$2"; shift 2
    local cmd out=""
    for cmd in "$@"; do
        if [[ "$cmd" == SLEEP\ * ]]; then
            sleep "${cmd#SLEEP }"
            continue
        fi
        cmd=${cmd//\{K3\}/${tag}c}
        cmd=${cmd//\{K2\}/${tag}b}
        cmd=${cmd//\{K\}/${tag}a}
        out+="> $cmd"$'\n'
        # --no-raw keeps null-vs-empty and bulk-vs-array distinguishable
        local reply status
        reply=$("$TIMEOUT_BIN" 5 redis-cli -p "$port" --no-raw $cmd 2>&1); status=$?
        [ "$status" -eq 124 ] && reply="<NO REPLY WITHIN 5s on :$port>"
        out+="$reply"$'\n'
    done
    printf '%s' "$out"
}

scenario() {
    local name="$1"; shift
    skip_filtered "$name" && return
    local tag="r${RUN}s${N}k"
    report "$name" "" \
        "$(run_seq "$MINE" "$tag" "$@")" \
        "$(run_seq "$REAL" "$tag" "$@")" \
        "yours (:$MINE)" "redis (:$REAL)"
}

# Sends raw bytes to both servers and diffs the reply bytes, for framing
# behaviour that redis-cli hides. $2 is a python bytes literal.
raw_scenario() {
    local name="$1" payload="$2"
    skip_filtered "$name" && return
    local a b
    a=$(python3 -c "import sys;sys.stdout.buffer.write($payload)" | nc -w 1 127.0.0.1 "$MINE" | xxd)
    b=$(python3 -c "import sys;sys.stdout.buffer.write($payload)" | nc -w 1 127.0.0.1 "$REAL" | xxd)
    report "$name" " ${DIM}(raw)${OFF}" "$a" "$b" "yours" "redis"
}

# Two writes with a pause between them, so one command spans two reads.
split_scenario() {
    local name="$1" first="$2" second="$3"
    skip_filtered "$name" && return
    local script='
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.sendall('"$first"')
time.sleep(0.4)
s.sendall('"$second"')
s.settimeout(1.5)
buf = b""
try:
    while True:
        c = s.recv(4096)
        if not c: break
        buf += c
        s.settimeout(0.3)   # the reply is in; only wait out a short quiet
except socket.timeout:
    pass
sys.stdout.write(repr(buf))
'
    report "$name" " ${DIM}(split)${OFF}" \
        "$(python3 -c "$script" "$MINE" 2>&1)" \
        "$(python3 -c "$script" "$REAL" 2>&1)" \
        "yours" "redis"
}

# One connection issues a blocking command; a second acts after $2 seconds.
# Commands are pipe-separated argument lists, semicolon-separated for several.
#   $1 name  $2 delay  $3 setup (or "")  $4 blocking cmd  $5 what the other conn does
block_scenario() {
    local name="$1" delay="$2" setup="$3" blocking="$4" meanwhile="$5"
    skip_filtered "$name" && return
    local tag="r${RUN}s${N}k"
    report "$name" " ${DIM}(block)${OFF}" \
        "$(python3 "$BLOCK_PY" "$MINE" "$tag" "$delay" "$setup" "$blocking" "$meanwhile" 2>&1)" \
        "$(python3 "$BLOCK_PY" "$REAL" "$tag" "$delay" "$setup" "$blocking" "$meanwhile" 2>&1)" \
        "yours" "redis"
}

# Two connections block, then a third acts. Diffs both blocked clients' replies.
#   $1 name  $2 delay  $3 first blocking cmd  $4 second blocking cmd  $5 trigger
block2_scenario() {
    local name="$1" delay="$2" b1="$3" b2="$4" trigger="$5"
    skip_filtered "$name" && return
    local tag="r${RUN}s${N}k"
    report "$name" " ${DIM}(block2)${OFF}" \
        "$(python3 "$BLOCK2_PY" "$MINE" "$tag" "$delay" "$b1" "$b2" "$trigger" 2>&1)" \
        "$(python3 "$BLOCK2_PY" "$REAL" "$tag" "$delay" "$b1" "$b2" "$trigger" 2>&1)" \
        "yours" "redis"
}

# Named connections driven step by step, for a connection that acts or
# disconnects while another is parked. Steps are described above CONN_PY.
conn_scenario() {
    local name="$1"; shift
    skip_filtered "$name" && return
    local tag="r${RUN}s${N}k"
    report "$name" " ${DIM}(conns)${OFF}" \
        "$(python3 "$CONN_PY" "$MINE" "$tag" "$@" 2>&1)" \
        "$(python3 "$CONN_PY" "$REAL" "$tag" "$@" 2>&1)" \
        "yours" "redis"
}

section() { printf '\n%s== %s%s\n' "$DIM" "$1" "$OFF"; }

# ---------------------------------------------------------------- scenarios

section "basics"
scenario "ping and echo" \
    "PING" "ping" "PiNg" "PING hello" "ECHO hi" "ECHO" "ECHO a b"

scenario "unknown command" "NOSUCHCOMMAND" "NOSUCHCOMMAND arg" "NOSUCHCOMMAND a b"

section "strings"
scenario "set and get" \
    "SET {K} hello" "GET {K}" "GET nosuchkey_{K}" "SET {K} world" "GET {K}"

scenario "type" \
    "SET {K} v" "RPUSH {K2} a" "TYPE {K}" "TYPE {K2}" "TYPE {K3}"

scenario "wrongtype both directions" \
    "SET {K} v" "RPUSH {K2} a" \
    "GET {K2}" "RPUSH {K} x" "LLEN {K}" "LRANGE {K} 0 -1" "LPOP {K}"

scenario "set clears an existing ttl" \
    "SET {K} v1 PX 50" "SET {K} v2" "SLEEP 0.3" "GET {K}"

section "expiry"
scenario "px expires" \
    "SET {K} v PX 100" "GET {K}" "SLEEP 0.5" "GET {K}" "EXISTS {K}"

scenario "ex is seconds not millis" \
    "SET {K} v EX 1" "GET {K}" "SLEEP 0.3" "GET {K}"

scenario "no ttl never expires" \
    "SET {K} v" "SLEEP 0.5" "GET {K}"

scenario "expiry option errors" \
    "SET {K} v PX" "SET {K} v PX abc" "SET {K} v PX 0" "SET {K} v PX -1" \
    "SET {K} v ZZ 5" "SET {K} v EX 0" "SET {K} v PX 100 EX 5"

scenario "write path reaps an expired key" \
    "SET {K} v PX 50" "SLEEP 0.3" "RPUSH {K} a" "TYPE {K}" "LRANGE {K} 0 -1"

section "incr"
scenario "incr increments a value set by set" \
    "SET {K} 5" "INCR {K}" "INCR {K}" "GET {K}" "TYPE {K}"

scenario "incr on a missing key starts at one" \
    "INCR {K}" "GET {K}" "TYPE {K}"

# Only the canonical form is stored as an integer, so these stay strings.
scenario "incr refuses a value that is not a canonical integer" \
    "SET {K} hello" "INCR {K}" \
    "SET {K} 010" "INCR {K}" "GET {K}" \
    "SET {K} +5" "INCR {K}" "SET {K} -0" "INCR {K}" "SET {K} 1.5" "INCR {K}"

scenario "incr at the maximum overflows and leaves the value" \
    "SET {K} 9223372036854775807" "INCR {K}" "GET {K}" \
    "SET {K} -9223372036854775808" "INCR {K}"

scenario "incr on a list is wrongtype" \
    "RPUSH {K} a" "INCR {K}" "LRANGE {K} 0 -1"

scenario "incr arity" \
    "INCR" "INCR {K} {K2}"

# INCR changes the value in place, so the deadline from SET still applies.
scenario "incr keeps the ttl" \
    "SET {K} 5 PX 200" "INCR {K}" "GET {K}" "SLEEP 0.4" "GET {K}"

scenario "incr on an expired key starts again with no ttl" \
    "SET {K} 5 PX 100" "SLEEP 0.3" "INCR {K}" "SLEEP 0.2" "GET {K}"

# run_seq cannot send an empty or space-led argument, so these go raw. The key
# is fixed rather than {K}, which is safe because SET overwrites it each run.
raw_scenario "incr on an empty string or a leading space" \
    "b'*3\r\n\$3\r\nSET\r\n\$9\r\nincr:raw1\r\n\$0\r\n\r\n*2\r\n\$4\r\nINCR\r\n\$9\r\nincr:raw1\r\n*3\r\n\$3\r\nSET\r\n\$9\r\nincr:raw1\r\n\$2\r\n 1\r\n*2\r\n\$4\r\nINCR\r\n\$9\r\nincr:raw1\r\n'"

section "lists"
scenario "rpush accumulates" \
    "RPUSH {K} a" "RPUSH {K} b c" "RPUSH {K} d e f" "LLEN {K}" "LRANGE {K} 0 -1" \
    "RPUSH {K}"

scenario "lpush reverses argument order" \
    "LPUSH {K} a b c" "LRANGE {K} 0 -1" "RPUSH {K} d" "LRANGE {K} 0 -1"

scenario "llen on missing key" "LLEN {K}" "LLEN nosuch_{K}"

scenario "lrange indexes" \
    "RPUSH {K} a b c d e" \
    "LRANGE {K} 0 -1" "LRANGE {K} 0 2" "LRANGE {K} -3 -1" "LRANGE {K} -100 100" \
    "LRANGE {K} 3 1" "LRANGE {K} 5 10" "LRANGE {K} -1 -5" "LRANGE {K} 0 0" \
    "LRANGE {K} -2 2" "LRANGE nosuch_{K} 0 -1" "LRANGE {K} abc 2"

scenario "lpop bare vs count" \
    "RPUSH {K} a b c d" \
    "LPOP {K}" "LPOP {K} 1" "LPOP {K} 0" "LPOP {K} 10" \
    "LPOP {K}" "LPOP nosuch_{K}" "LPOP nosuch_{K} 2"

scenario "rpop returns in pop order" \
    "RPUSH {K} a b c" "RPOP {K} 2" "LRANGE {K} 0 -1"

scenario "pop count errors" \
    "RPUSH {K} a" "LPOP {K} -1" "LPOP {K} abc" "LPOP {K} 1 2"

scenario "emptying a list deletes the key (lpop)" \
    "RPUSH {K} a" "LPOP {K}" "EXISTS {K}" "TYPE {K}"

scenario "emptying a list deletes the key (rpop)" \
    "RPUSH {K} a" "RPOP {K}" "EXISTS {K}" "TYPE {K}"

scenario "emptying a list deletes the key (count)" \
    "RPUSH {K} a b" "LPOP {K} 5" "EXISTS {K}" "TYPE {K}"

section "lmove"
scenario "lmove basics" \
    "RPUSH {K} a b c" "LMOVE {K} {K2} LEFT RIGHT" \
    "LRANGE {K} 0 -1" "LRANGE {K2} 0 -1" \
    "LMOVE {K} {K2} RIGHT LEFT" "LRANGE {K} 0 -1" "LRANGE {K2} 0 -1"

scenario "lmove missing source creates nothing" \
    "LMOVE nosuch_{K} {K2} LEFT RIGHT" "EXISTS {K2}"

scenario "lmove wrongtype leaves source intact" \
    "RPUSH {K} a b" "SET {K2} str" \
    "LMOVE {K} {K2} LEFT RIGHT" "LRANGE {K} 0 -1" \
    "LMOVE {K2} {K} LEFT RIGHT" "LRANGE {K} 0 -1"

scenario "lmove same key rotates" \
    "RPUSH {K} a b c" \
    "LMOVE {K} {K} LEFT RIGHT" "LRANGE {K} 0 -1" \
    "LMOVE {K} {K} RIGHT LEFT" "LRANGE {K} 0 -1"

scenario "lmove single element same key" \
    "RPUSH {K} a" "LMOVE {K} {K} LEFT RIGHT" "LRANGE {K} 0 -1" "EXISTS {K}"

scenario "lmove syntax and arity" \
    "RPUSH {K} a" "LMOVE {K} {K2} SIDEWAYS RIGHT" "LMOVE {K} {K2} LEFT" \
    "LMOVE {K} {K2} left right" "LRANGE {K2} 0 -1"


section "streams"
scenario "xadd explicit ids" \
    "XADD {K} 0-0 f v" "XADD {K} 0-1 f v" "XADD {K} 5-5 f v" \
    "XADD {K} 3-0 f v" "XADD {K} 5-5 f v" "XADD {K} 5-6 f v" "TYPE {K}"

scenario "xadd bare millisecond is sequence zero" \
    "XADD {K} 7 f v" "XADD {K} 7 f v" "XADD {K} 8 f v"

scenario "xadd auto sequence" \
    "XADD {K} 5-* f v" "XADD {K} 5-* f v" "XADD {K} 6-* f v" "XADD {K} 5-* f v"

scenario "xadd malformed ids" \
    "XADD {K} abc f v" "XADD {K} 5-abc f v" "XADD {K} -1 f v" "XADD {K} 5- f v"

scenario "xadd arity" \
    "XADD {K}" "XADD {K} 9-1" "XADD {K} 9-1 f"

scenario "xadd wrongtype" \
    "SET {K} str" "XADD {K} 1-1 f v" "GET {K}"


scenario "xrange bounds" \
    "XADD {K} 5-0 a 1" "XADD {K} 5-1 b 2" "XADD {K} 6-0 c 3" \
    "XRANGE {K} - +" "XRANGE {K} 5 5" "XRANGE {K} 6 6" \
    "XRANGE {K} 5-1 6" "XRANGE {K} 5-0 5-0" \
    "XRANGE {K} 9 10" "XRANGE {K} 6 5" "XRANGE nosuch_{K} - +"

scenario "xrange count" \
    "XADD {K} 1-0 a 1" "XADD {K} 2-0 b 2" "XADD {K} 3-0 c 3" \
    "XRANGE {K} - + COUNT 2" "XRANGE {K} - + COUNT 0" \
    "XRANGE {K} - + COUNT -1" "XRANGE {K} - + COUNT 99" \
    "XRANGE {K} - + count 2"

scenario "xrange syntax errors" \
    "XADD {K} 1-0 a 1" \
    "XRANGE {K}" "XRANGE {K} -" "XRANGE {K} - + BADKW" \
    "XRANGE {K} - + BADKW 2" "XRANGE {K} - + COUNT" "XRANGE {K} - + COUNT abc" \
    "XRANGE {K} abc +" "XRANGE {K} - abc"

scenario "xrange preserves field order and duplicates" \
    "XADD {K} 1-1 b 2 a 1 b 3" "XRANGE {K} - +"

scenario "xrange wrongtype" \
    "SET {K} str" "XRANGE {K} - +"


scenario "xread single stream" \
    "XADD {K} 1-0 a 1" "XADD {K} 2-0 b 2" "XADD {K} 3-0 c 3" \
    "XREAD STREAMS {K} 0" "XREAD STREAMS {K} 1-0" "XREAD STREAMS {K} 3-0" \
    "XREAD STREAMS {K} 9-9" "XREAD STREAMS nosuch_{K} 0"

scenario "xread ids are exclusive" \
    "XADD {K} 5-0 f v" "XREAD STREAMS {K} 5-0" "XREAD STREAMS {K} 4-0" \
    "XREAD STREAMS {K} 5" "XREAD STREAMS {K} 4"

scenario "xread multiple streams" \
    "XADD {K} 1-0 a 1" "XADD {K2} 5-0 b 2" \
    "XREAD STREAMS {K} {K2} 0 0" \
    "XREAD STREAMS {K} {K2} 9-9 0" \
    "XREAD STREAMS {K} {K2} 9-9 9-9" \
    "XREAD STREAMS {K} nosuch_{K} 0 0"

scenario "xread count" \
    "XADD {K} 1-0 a 1" "XADD {K} 2-0 b 2" "XADD {K} 3-0 c 3" \
    "XREAD COUNT 2 STREAMS {K} 0" "XREAD COUNT 0 STREAMS {K} 0" \
    "XREAD COUNT -1 STREAMS {K} 0" "XREAD COUNT 99 STREAMS {K} 0"

scenario "xread option order is interchangeable" \
    "XADD {K} 1-0 a 1" \
    "XREAD COUNT 1 BLOCK 10 STREAMS {K} 0" \
    "XREAD BLOCK 10 COUNT 1 STREAMS {K} 0"

scenario "xread plus returns the last entry" \
    "XADD {K} 1-0 a 1" "XADD {K} 2-0 b 2" \
    "XREAD STREAMS {K} +" "XREAD COUNT 5 STREAMS {K} +" \
    "XREAD STREAMS nosuch_{K} +"

scenario "xread count ignored for plus but not for others" \
    "XADD {K} 1-0 a 1" "XADD {K} 2-0 b 2" \
    "XADD {K2} 1-0 c 1" "XADD {K2} 2-0 d 2" \
    "XREAD COUNT 1 STREAMS {K} {K2} 0 +"

scenario "xread dollar without block returns nil" \
    "XADD {K} 1-0 a 1" "XREAD STREAMS {K} \$"

scenario "xread syntax errors" \
    "XADD {K} 1-0 a 1" \
    "XREAD" "XREAD STREAMS" "XREAD STREAMS {K}" "XREAD STREAMS {K} {K2} 0" \
    "XREAD {K} 0" "XREAD BADKW STREAMS {K} 0" \
    "XREAD COUNT STREAMS {K} 0" "XREAD BLOCK abc STREAMS {K} 0" \
    "XREAD BLOCK -1 STREAMS {K} 0" "XREAD COUNT abc STREAMS {K} 0"

scenario "xread wrongtype" \
    "SET {K} str" "XREAD STREAMS {K} 0" "XREAD STREAMS {K} +"

scenario "xread same stream twice" \
    "XADD {K} 1-0 a 1" "XADD {K} 2-0 b 2" \
    "XREAD STREAMS {K} {K} 0 1-0"

section "xtrim"
# Explicit ids throughout: run_seq expands commands unquoted, so a bare * would
# become the file names in the current directory.
FIVE=("XADD {K} 1-0 f v" "XADD {K} 2-0 f v" "XADD {K} 3-0 f v" "XADD {K} 4-0 f v" "XADD {K} 5-0 f v")

scenario "xtrim maxlen trims exactly" \
    "${FIVE[@]}" "XTRIM {K} MAXLEN 2" "XRANGE {K} - +"

scenario "xtrim maxlen with an equals sign" \
    "${FIVE[@]}" "XTRIM {K} MAXLEN = 2" "XLEN {K}"

scenario "xtrim maxlen above the length removes nothing" \
    "${FIVE[@]}" "XTRIM {K} MAXLEN 10" "XLEN {K}"

scenario "xtrim to zero keeps the key and the last id" \
    "${FIVE[@]}" "XTRIM {K} MAXLEN 0" "XLEN {K}" "EXISTS {K}" \
    "XADD {K} 1-0 f v" "XADD {K} 6-0 f v"

scenario "xtrim minid" \
    "${FIVE[@]}" "XTRIM {K} MINID 3-0" "XRANGE {K} - +" \
    "XTRIM {K} MINID 4" "XRANGE {K} - +"

scenario "xtrim on a missing key" \
    "XTRIM {K} MAXLEN 1" "EXISTS {K}"

# The XLEN at the end shows that no failed form trimmed anything.
scenario "xtrim errors" \
    "${FIVE[@]}" "XTRIM {K}" "XTRIM {K} MAXLEN" \
    "XTRIM {K} MAXLEN -1" "XTRIM {K} MAXLEN abc" "XTRIM {K} MAXLEN 010" \
    "XTRIM {K} MAXLEN 2 LIMIT 10" "XTRIM {K} MAXLEN 2 MINID 1" \
    "XTRIM {K} MINID abc" "XTRIM {K} FOO 1" "XTRIM {K} MAXLEN 2 extra" \
    "XLEN {K}"

scenario "xtrim on a string is wrongtype" \
    "SET {K} x" "XTRIM {K} MAXLEN 1"

# Consumer group options. With no groups they change nothing.
scenario "xtrim accepts the reference options" \
    "${FIVE[@]}" "XTRIM {K} MAXLEN 4 KEEPREF" "XTRIM {K} KEEPREF MAXLEN 3" \
    "XTRIM {K} MAXLEN 2 DELREF" "XTRIM {K} MAXLEN 1 ACKED" "XLEN {K}"

scenario "xadd trims after adding" \
    "${FIVE[@]}" "XADD {K} MAXLEN 2 6-0 f v" "XRANGE {K} - +"

scenario "xadd minid" \
    "${FIVE[@]}" "XADD {K} MINID 4 6-0 f v" "XRANGE {K} - +"

scenario "xadd maxlen zero adds then trims everything" \
    "${FIVE[@]}" "XADD {K} MAXLEN 0 6-0 f v" "XLEN {K}" "XADD {K} 6-0 f v"

# Options end at the id. After it, MAXLEN is just a field name.
scenario "xadd options after the id are fields" \
    "XADD {K} 6-0 MAXLEN 2 f v" "XRANGE {K} - +"

scenario "xadd nomkstream" \
    "XADD {K} NOMKSTREAM 1-0 f v" "EXISTS {K}" \
    "XADD {K} NOMKSTREAM MAXLEN 1 1-0 f v" "XADD {K} MAXLEN 1 NOMKSTREAM 1-0 f v" \
    "EXISTS {K}" "XADD {K} 1-0 f v" "XADD {K} NOMKSTREAM 2-0 f v" "XLEN {K}"

# Trim arguments are checked before the id, so MAXLEN -1 wins over 0-0.
scenario "xadd checks the trim clause before the id" \
    "${FIVE[@]}" "XADD {K} MAXLEN -1 0-0 f v" "XADD {K} MAXLEN abc 6-0 f v" \
    "XADD {K} MAXLEN 2 LIMIT 5 6-0 f v" "XADD {K} MAXLEN 2 6-0 f" \
    "XADD {K} MAXLEN 2" "XADD {K} MAXLEN 2 6-0" "XADD {K} MAXLEN 2 0-0 f v" \
    "XLEN {K}"

scenario "xadd accepts the reference options" \
    "${FIVE[@]}" "XADD {K} KEEPREF MAXLEN 2 6-0 f v" \
    "XADD {K} MAXLEN 2 KEEPREF 7-0 f v" "XLEN {K}"

# XADD adds and trims inside one command, so a reader blocked on $ finds
# nothing new when it is served and stays parked until its timeout.
conn_scenario "a blocked xread gets nothing when xadd trims its entry away" \
    "a send XREAD|BLOCK|1000|STREAMS|{K}|\$" "sleep 0.2" \
    "b send XADD|{K}|MAXLEN|0|5-0|f|v" "b read" \
    "sleep 1.0" "a read"

# Redis trims ~ by whole macro nodes of up to 100 entries, so on a short stream
# it removes nothing, while this server trims exactly until phase 5 has buckets.
# STRICT=1 ./test.sh to see the diffs.
if [ "${STRICT:-0}" = 1 ]; then
    scenario "xtrim approximate maxlen" \
        "${FIVE[@]}" "XTRIM {K} MAXLEN ~ 2" "XLEN {K}"
    scenario "xtrim approximate with a limit" \
        "${FIVE[@]}" "XTRIM {K} MAXLEN ~ 2 LIMIT 10" "XLEN {K}"
    scenario "xadd approximate maxlen" \
        "${FIVE[@]}" "XADD {K} MAXLEN ~ 2 6-0 f v" "XLEN {K}"
fi

section "blocking"
# A push arrives while a client is parked. The reply must be [key, element].
block_scenario "blpop woken by a later push" 0.5 "" \
    "BLPOP|{K}|0" \
    "RPUSH|{K}|a"

block_scenario "brpop takes from the tail" 0.5 "" \
    "BRPOP|{K}|0" \
    "RPUSH|{K}|a|b"

# Nothing arrives. The reply must be the null ARRAY, *-1, not $-1.
block_scenario "blpop times out with a null array" 2.5 "" \
    "BLPOP|{K}|1" \
    "PING"

# The list already has data, so this must not block at all.
block_scenario "blpop returns immediately when data exists" 1.5 "" \
    "BLPOP|{K}|0" \
    "PING"

# Multiple keys: the first non-empty one wins, and the reply names it.
block_scenario "blpop scans past an empty key" 0.5 "" \
    "BLPOP|{K}|{K2}|0" \
    "RPUSH|{K2}|x"

# The element must survive a push that lands after the client gave up.
# This is the data-loss case that is_closed() exists to prevent.
block_scenario "element survives a push after the timeout" 2 "" \
    "BLPOP|{K}|1" \
    "RPUSH|{K}|a;LRANGE|{K}|0|-1"

# A waiter blocked on a key that becomes a string gets WRONGTYPE, not silence.
block_scenario "wrongtype while parked" 0.5 "" \
    "BLPOP|{K}|2" \
    "SET|{K}|str"

block_scenario "blmove woken by a push to the source" 0.5 "" \
    "BLMOVE|{K}|{K2}|LEFT|RIGHT|0" \
    "RPUSH|{K}|x;LRANGE|{K2}|0|-1"

# Timeout reply must be a null BULK STRING, not a null array.
block_scenario "blmove times out with a null bulk string" 2.5 "" \
    "BLMOVE|{K}|{K2}|LEFT|RIGHT|1" \
    "PING"

# Destination is a string. Does Redis block or error immediately?
block_scenario "blmove with a wrongtype destination" 2.5 "" \
    "BLMOVE|{K}|{K2}|LEFT|RIGHT|1" \
    "SET|{K2}|str"

# The cascade: one push satisfies a BLMOVE, whose push then satisfies a BLPOP.
block2_scenario "blmove cascade wakes a waiter on the destination" 1 \
    "BLMOVE|{K}|{K2}|LEFT|RIGHT|0" \
    "BLPOP|{K2}|0" \
    "RPUSH|{K}|x"

# FIFO across two waiters on one key, one element each.
block2_scenario "two waiters served in block order" 1 \
    "BLPOP|{K}|0" \
    "BLPOP|{K}|0" \
    "RPUSH|{K}|a|b"

# One element, two waiters. The second must stay parked and time out.
block2_scenario "one element serves only the first waiter" 1 \
    "BLPOP|{K}|0" \
    "BLPOP|{K}|2" \
    "RPUSH|{K}|only"

block2_scenario "xread serves waiters with different ids" 1 \
    "XREAD|BLOCK|0|STREAMS|{K}|9-9" \
    "XREAD|BLOCK|0|STREAMS|{K}|0-0" \
    "XADD|{K}|5-0|f|v"

block_scenario "xread blocks then wakes on xadd" 0.5 "" \
    "XREAD|BLOCK|0|STREAMS|{K}|0" \
    "XADD|{K}|5-0|f|v"

block_scenario "xread times out with a null array" 2.5 "" \
    "XREAD|BLOCK|1000|STREAMS|{K}|0" \
    "PING"

block_scenario "xread blocks on one of several streams" 0.5 "" \
    "XREAD|BLOCK|0|STREAMS|{K}|{K2}|0|0" \
    "XADD|{K2}|5-0|f|v"

block_scenario "xread dollar skips pre-existing entries" 0.5 \
    "XADD|{K}|1-0|old|1" \
    "XREAD|BLOCK|0|STREAMS|{K}|\$" \
    "XADD|{K}|5-0|new|2"

block2_scenario "xread fans out to all waiters" 1 \
    "XREAD|BLOCK|0|STREAMS|{K}|0-0" \
    "XREAD|BLOCK|0|STREAMS|{K}|0-0" \
    "XADD|{K}|5-0|f|v"

section "protocol framing"
raw_scenario "pipelined commands in one packet" \
    "b'*1\r\n\$4\r\nPING\r\n*1\r\n\$4\r\nping\r\n*1\r\n\$4\r\nPiNg\r\n'"

# An empty array gets no reply at all, so follow it with a PING: otherwise both
# sides are empty and the harness cannot tell a pass from a dead connection.
raw_scenario "empty array" \
    "b'*0\r\n*1\r\n\$4\r\nPING\r\n'"

split_scenario "command split across two reads" \
    "b'*1\r\n\$4\r\nPI'" "b'NG\r\n'"

split_scenario "bulk payload split mid value" \
    "b'*3\r\n\$3\r\nSET\r\n\$5\r\nsplit\r\n\$5\r\nab'" "b'cde\r\n'"

section "argument bounds and panic bait"
# Each of these reached a panicking std constructor before the fix. A missing
# reply on the MINE side of a diff means the task died rather than answered.

scenario "expiry argument overflow" \
    "SET {K} v EX 9223372036854775807" "GET {K}" \
    "SET {K} v EX 9223372036854776" \
    "SET {K} v EX 9223372036854775808"

# Redis rejects a deadline past i64::MAX with an overflow check that relies on
# signed wraparound, undefined behaviour in C. Some builds (Homebrew on macOS,
# at least) compile it out and reply OK with a key that is already expired, so
# the oracle is only trusted here if it still rejects the argument.
if [ "$(redis-cli -p "$REAL" SET "r${RUN}probe" v PX 9223372036854775807)" = OK ]; then
    skip_filtered "px deadline overflow" || {
        SKIP=$((SKIP + 1))
        printf '%s  SKIP%s  px deadline overflow %s(this redis-server has no overflow check)%s\n' \
            "$YEL" "$OFF" "$DIM" "$OFF"
    }
else
    scenario "px deadline overflow" \
        "SET {K} v PX 9223372036854775807" "GET {K}"
fi

scenario "blpop timeout argument edges" \
    "RPUSH {K} a b c d e f g" \
    "BLPOP {K} nan" "BLPOP {K} inf" "BLPOP {K} -inf" \
    "BLPOP {K} 1e300" "BLPOP {K} -1" "BLPOP {K} abc" "BLPOP {K} -0" \
    "LRANGE {K} 0 -1"

scenario "xread block argument edges" \
    "XREAD BLOCK 9223372036854775807 STREAMS {K} 0" \
    "XREAD BLOCK 9223372036854775808 STREAMS {K} 0" \
    "XREAD BLOCK 1.5 STREAMS {K} 0"

section "creation and arity ordering"

scenario "xadd id zero creates nothing" \
    "XADD {K} 0-0 f v" "EXISTS {K}" "TYPE {K}" "XLEN {K}"

scenario "xdel arity and semantics" \
    "XADD {K} 1-1 f v" "XDEL {K}" "XDEL {K} 1-1" "XDEL {K} 1-1" \
    "XDEL {K} 9-9" "EXISTS {K}" "TYPE {K}" "XLEN {K}" "XDEL nosuch_{K} 1-1"

scenario "xadd arity is checked before the id" \
    "XADD {K} abc" "XADD {K} abc f" "XADD {K} 1-1 f"

scenario "xrange reports a bad id before a bad count" \
    "XADD {K} 1-0 a 1" \
    "XRANGE {K} badid badid COUNT abc" "XRANGE {K} badid + COUNT 1"

section "strict integer parsing"
# Redis parses integer arguments with string2ll, which rejects a leading plus
# and leading zeros. Rust's FromStr accepts both. Every diff here is one bug.

scenario "leading plus and zeros in integer arguments" \
    "SET {K} v EX +10" "SET {K} v EX 010" "SET {K} v PX 010" \
    "RPUSH {K2} a b c" "LPOP {K2} +1" "LPOP {K2} 01" \
    "LRANGE {K2} +0 -1"

scenario "leading plus and zeros in stream ids" \
    "XADD {K} +5-1 f v" "XADD {K} 05-1 f v" "XADD {K} 5-01 f v" \
    "XRANGE {K} +5 +6" \
    "XREAD COUNT 010 STREAMS {K} 0" "XREAD BLOCK 010 STREAMS {K} 0"

section "parked connections"
# Phase 3 step 2 exit criteria. A parked client's later commands run only after
# it is served, and a client that leaves while parked must not take an element.

conn_scenario "commands pipelined behind a parked one wait for it" \
    "a send PING;BLPOP|{K}|0;SET|{K2}|1" "a read" \
    "b send GET|{K2}" "b read" \
    "b send RPUSH|{K}|v" "b read" "a read" \
    "b send GET|{K2}" "b read"

conn_scenario "commands sent while parked wait for it" \
    "a send BLPOP|{K}|0" "sleep 0.2" "a send SET|{K2}|1" "a read" \
    "b send GET|{K2}" "b read" \
    "b send RPUSH|{K}|v" "b read" "a read" \
    "b send GET|{K2}" "b read"

conn_scenario "a client that leaves while parked takes nothing" \
    "a send BLPOP|{K}|0" "sleep 0.2" "a close" "sleep 0.2" \
    "b send RPUSH|{K}|v" "b read" \
    "b send LRANGE|{K}|0|-1" "b read"

conn_scenario "a client that leaves during a timed block takes nothing" \
    "a send BLPOP|{K}|5" "sleep 0.2" "a close" "sleep 0.2" \
    "b send RPUSH|{K}|v" "b read" \
    "b send LRANGE|{K}|0|-1" "b read"

conn_scenario "errors sent while parked wait for it" \
    "a send BLPOP|{K}|0" "sleep 0.2" "a send GET" "a read" \
    "b send RPUSH|{K}|v" "b read" "a read"

# The drivers stop listening shortly after a scenario's last event, so this is
# the one place that checks nothing arrives later on its own: a timed block
# that was served must not also time out.
conn_scenario "a served timed block sends nothing when its timeout passes" \
    "a send BLPOP|{K}|1" "sleep 0.2" \
    "b send RPUSH|{K}|v" "b read" "a read" \
    "sleep 1.0" "a read"

section "transaction correctness"
# A transaction lives on one connection, so these are conn_scenarios: run_seq
# opens a new redis-cli connection per command. Most send the whole sequence in
# one write and read every reply back in order.

conn_scenario "multi queues and exec runs them in order" \
    "a send MULTI;SET|{K}|1;INCR|{K};GET|{K};EXEC" "a read"

conn_scenario "an empty transaction" \
    "a send MULTI;EXEC" "a read"

conn_scenario "exec and discard without multi" \
    "a send EXEC;DISCARD" "a read"

# A rejected EXEC is EXECABORT even outside MULTI, and inside one it discards
# the transaction, so the queued SET never runs and the next EXEC has no MULTI.
conn_scenario "multi command arity" \
    "a send MULTI|x;EXEC|x;DISCARD|x" "a read" \
    "a send MULTI;SET|{K}|1;EXEC|x;EXEC;GET|{K}" "a read"

# The nested MULTI is an error but does not fail the transaction.
conn_scenario "multi cannot be nested" \
    "a send MULTI;MULTI;SET|{K}|1;EXEC" "a read"

conn_scenario "discard drops the queue" \
    "a send MULTI;SET|{K}|1;DISCARD;GET|{K};EXEC" "a read"

# Only a wrong argument count and an unknown command fail at queue time. Later
# commands still reply QUEUED, and EXEC refuses the whole transaction.
conn_scenario "a wrong argument count aborts the transaction" \
    "a send MULTI;SET|{K}|1;GET;PING;EXEC;GET|{K}" "a read"

conn_scenario "an unknown command aborts the transaction" \
    "a send MULTI;NOSUCH|x;EXEC" "a read"

# Every other error waits for EXEC, fills its own slot, and the rest still run.
conn_scenario "errors at exec time do not abort the rest" \
    "a send MULTI;SET|{K}|x;LPUSH|{K}|a;INCR|{K};SET|{K}|2;EXEC;GET|{K}" "a read"

conn_scenario "a syntax error is queued and returned by exec" \
    "a send MULTI;SET|{K}|v|ZZ;SET|{K2}|1;EXEC;GET|{K2}" "a read"

conn_scenario "blocking commands inside multi do not block" \
    "a send MULTI;BLPOP|{K}|0;XREAD|BLOCK|0|STREAMS|{K2}|\$;EXEC" "a read"

# EXEC runs as one step: a client blocked on a key the transaction pushes to is
# served only after the whole transaction, so LLEN inside it sees both.
conn_scenario "a transaction wakes a blocked client only after exec" \
    "b send BLPOP|{K}|5" "sleep 0.2" \
    "a send MULTI;RPUSH|{K}|x|y;LLEN|{K}" "a read" "b read" \
    "a send EXEC" "a read" "b read" \
    "a send LRANGE|{K}|0|-1" "a read"

# No isolation before EXEC: queued commands see the data as it is when it runs.
conn_scenario "another client runs between multi and exec" \
    "a send SET|{K}|1" "a read" "a send MULTI;INCR|{K}" "a read" \
    "b send SET|{K}|10" "b read" "a send EXEC" "a read"

# Deliberate divergences from real Redis, documented rather than fixed:
#   - no inline command support: Redis parses input not starting with '*' as a
#     space-separated inline command, and skips 2 bytes after a bulk payload
#     without checking they are CRLF. We are stricter on the second.
#   - error messages escape CR and LF rather than substituting spaces.
#   - "expected a bulk string" rather than "expected '$', got ':'".
# STRICT=1 ./test.sh to see the diffs.
if [ "${STRICT:-0}" = 1 ]; then
    section "known divergences"
    raw_scenario "protocol error closes the connection" "b'X\r\n'"
    raw_scenario "bad bulk length is a protocol error" "b'*1\r\n\$1\r\nab\r\n'"
    raw_scenario "non bulk string element" "b'*1\r\n:5\r\n'"
    raw_scenario "crlf in a command name cannot split the reply" \
        "b'*1\r\n\$14\r\nFOO\r\n+INJECTED\r\n'"
fi

# ---------------------------------------------------------------- summary

printf '\n'
if [ "$FAIL" -eq 0 ]; then
    printf '%s%d passed%s' "$GRN" "$PASS" "$OFF"
else
    printf '%s%d passed, %d failed%s' "$RED" "$PASS" "$FAIL" "$OFF"
fi
[ "$SKIP" -gt 0 ] && printf ', %d skipped' "$SKIP"
printf '\n'

if [ "$FAIL" -gt 0 ]; then
    printf '\nfailed:\n'
    for n in "${FAILED_NAMES[@]}"; do printf '  %s\n' "$n"; done
    printf '\n%sA diff is not automatically a bug: some scenarios use commands you\n' "$DIM"
    printf 'have not implemented yet. Read both sides before changing anything.%s\n' "$OFF"
    exit 1
fi