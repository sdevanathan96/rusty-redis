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

TMP=$(mktemp -d)
CONN_PY="$TMP/conn.py"

cleanup() {
    rm -rf "$TMP"
    [ "$OUR_REDIS" = 1 ] && redis-cli -p "$REAL" SHUTDOWN NOSAVE >/dev/null 2>&1
    return 0
}
trap cleanup EXIT

# Driver for scripted multi-connection scenarios. Each argument after the tag
# is one step: "<conn> send <cmds>", "<conn> read [secs]", "<conn> close", or
# "sleep <secs>". A connection opens on its first send. It lives in a file
# because quotes nested inside a function once silently lost half its code.
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

conns, sent = {}, {}
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
        sent[name] = time.monotonic()
    elif action == "read":
        # Waits 0.5s for anything, or with secs until secs after this
        # connection's last send, so b'' means nothing arrived in that time.
        # Then stops after 0.2s of quiet once something has.
        s = conns[name]
        wait = sent[name] + float(words[2]) - time.monotonic() if len(words) > 2 else 0.5
        s.settimeout(max(wait, 0.05))
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

# Runs "$1 <port> ${@:2}" against both servers at once, into OUT_MINE and
# OUT_REAL. Like $(...), reading them back strips trailing newlines.
on_both() {
    "$1" "$MINE" "${@:2}" > "$TMP/mine" &
    local pid=$!
    "$1" "$REAL" "${@:2}" > "$TMP/real"
    wait "$pid"
    OUT_MINE=$(<"$TMP/mine")
    OUT_REAL=$(<"$TMP/real")
}

scenario() {
    local name="$1"; shift
    skip_filtered "$name" && return
    on_both run_seq "r${RUN}s${N}k" "$@"
    report "$name" "" "$OUT_MINE" "$OUT_REAL" "yours (:$MINE)" "redis (:$REAL)"
}

# Sends raw bytes to both servers and diffs the reply bytes, for framing
# behaviour that redis-cli hides. $2 is a python bytes literal.
raw_run() {
    python3 -c "import sys;sys.stdout.buffer.write($2)" | nc -w 1 127.0.0.1 "$1" | xxd
}

raw_scenario() {
    local name="$1" payload="$2"
    skip_filtered "$name" && return
    on_both raw_run "$payload"
    report "$name" " ${DIM}(raw)${OFF}" "$OUT_MINE" "$OUT_REAL" "yours" "redis"
}

# Two writes with a pause between them, so one command spans two reads.
split_run() { python3 -c "$2" "$1" 2>&1; }

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
    on_both split_run "$script"
    report "$name" " ${DIM}(split)${OFF}" "$OUT_MINE" "$OUT_REAL" "yours" "redis"
}

# Named connections driven step by step, as described above CONN_PY.
conn_run() { python3 "$CONN_PY" "$@" 2>&1; }

conn_scenario() {
    local name="$1"; shift
    skip_filtered "$name" && return
    on_both conn_run "r${RUN}s${N}k" "$@"
    report "$name" " ${DIM}(conns)${OFF}" "$OUT_MINE" "$OUT_REAL" "yours" "redis"
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
# Client a parks and b acts after a pause. "a read N" gives a's reply until N
# seconds after a sent, which is 3s after the pause.
# A push arrives while a client is parked. The reply must be [key, element].
conn_scenario "blpop woken by a later push" \
    "a send BLPOP|{K}|0" "sleep 0.5" "b send RPUSH|{K}|a" "b read" "a read 3.5"

conn_scenario "brpop takes from the tail" \
    "a send BRPOP|{K}|0" "sleep 0.5" "b send RPUSH|{K}|a|b" "b read" "a read 3.5"

# Nothing arrives. The reply must be the null ARRAY, *-1, not $-1.
conn_scenario "blpop times out with a null array" \
    "a send BLPOP|{K}|1" "sleep 2.5" "b send PING" "b read" "a read 5.5"

# The list already has data, so this must not block at all.
conn_scenario "blpop returns immediately when data exists" \
    "a send RPUSH|{K}|a;BLPOP|{K}|0" "a read"

# Multiple keys: the first non-empty one wins, and the reply names it.
conn_scenario "blpop scans past an empty key" \
    "a send BLPOP|{K}|{K2}|0" "sleep 0.5" "b send RPUSH|{K2}|x" "b read" "a read 3.5"

# The element must survive a push that lands after the client gave up: the
# LRANGE must still show it, and the timed-out client must get nothing more.
# This is the race Unpark settles.
conn_scenario "element survives a push after the timeout" \
    "a send BLPOP|{K}|1" "sleep 1.3" "a read" \
    "b send RPUSH|{K}|a;LRANGE|{K}|0|-1" "b read" "a read"

# A waiter blocked on a key that becomes a string gets WRONGTYPE, not silence.
conn_scenario "wrongtype while parked" \
    "a send BLPOP|{K}|2" "sleep 0.5" "b send SET|{K}|str" "b read" "a read 3.5"

conn_scenario "blmove woken by a push to the source" \
    "a send BLMOVE|{K}|{K2}|LEFT|RIGHT|0" "sleep 0.5" \
    "b send RPUSH|{K}|x;LRANGE|{K2}|0|-1" "b read" "a read 3.5"

# Timeout reply must be a null BULK STRING, not a null array.
conn_scenario "blmove times out with a null bulk string" \
    "a send BLMOVE|{K}|{K2}|LEFT|RIGHT|1" "sleep 2.5" "b send PING" "b read" "a read 5.5"

# Destination is a string. Does Redis block or error immediately?
conn_scenario "blmove with a wrongtype destination" \
    "a send BLMOVE|{K}|{K2}|LEFT|RIGHT|1" "sleep 2.5" \
    "b send SET|{K2}|str" "b read" "a read 5.5"

# Two clients park, a before b so FIFO order is fixed, then c acts.
# Each reply gets until 4s after its own client sent.
# The cascade: one push satisfies a BLMOVE, whose push then satisfies a BLPOP.
conn_scenario "blmove cascade wakes a waiter on the destination" \
    "a send BLMOVE|{K}|{K2}|LEFT|RIGHT|0" "sleep 0.3" "b send BLPOP|{K2}|0" "sleep 1.3" \
    "c send RPUSH|{K}|x" "c read" "a read 4" "b read 4"

# FIFO across two waiters on one key, one element each.
conn_scenario "two waiters served in block order" \
    "a send BLPOP|{K}|0" "sleep 0.3" "b send BLPOP|{K}|0" "sleep 1.3" \
    "c send RPUSH|{K}|a|b" "c read" "a read 4" "b read 4"

# One element, two waiters. The second must stay parked and time out.
conn_scenario "one element serves only the first waiter" \
    "a send BLPOP|{K}|0" "sleep 0.3" "b send BLPOP|{K}|2" "sleep 1.3" \
    "c send RPUSH|{K}|only" "c read" "a read 4" "b read 4"

conn_scenario "xread serves waiters with different ids" \
    "a send XREAD|BLOCK|0|STREAMS|{K}|9-9" "sleep 0.3" \
    "b send XREAD|BLOCK|0|STREAMS|{K}|0-0" "sleep 1.3" \
    "c send XADD|{K}|5-0|f|v" "c read" "a read 4" "b read 4"

conn_scenario "xread blocks then wakes on xadd" \
    "a send XREAD|BLOCK|0|STREAMS|{K}|0" "sleep 0.5" \
    "b send XADD|{K}|5-0|f|v" "b read" "a read 3.5"

conn_scenario "xread times out with a null array" \
    "a send XREAD|BLOCK|1000|STREAMS|{K}|0" "sleep 2.5" "b send PING" "b read" "a read 5.5"

conn_scenario "xread blocks on one of several streams" \
    "a send XREAD|BLOCK|0|STREAMS|{K}|{K2}|0|0" "sleep 0.5" \
    "b send XADD|{K2}|5-0|f|v" "b read" "a read 3.5"

conn_scenario "xread dollar skips pre-existing entries" \
    "b send XADD|{K}|1-0|old|1" "b read" \
    "a send XREAD|BLOCK|0|STREAMS|{K}|\$" "sleep 0.5" \
    "b send XADD|{K}|5-0|new|2" "b read" "a read 3.5"

conn_scenario "xread fans out to all waiters" \
    "a send XREAD|BLOCK|0|STREAMS|{K}|0-0" "sleep 0.3" \
    "b send XREAD|BLOCK|0|STREAMS|{K}|0-0" "sleep 1.3" \
    "c send XADD|{K}|5-0|f|v" "c read" "a read 4" "b read 4"

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

section "watch"
# Each read before another client's write makes sure the server has handled
# the WATCH first: two connections are not ordered otherwise.

conn_scenario "a write by another client aborts exec" \
    "a send WATCH|{K}" "a read" "b send SET|{K}|1" "b read" \
    "a send MULTI;SET|{K}|2;EXEC;GET|{K}" "a read"

conn_scenario "reads and writes to other keys do not abort" \
    "a send WATCH|{K}" "a read" \
    "b send GET|{K};TYPE|{K};EXISTS|{K};SET|{K2}|v" "b read" \
    "a send MULTI;PING;EXEC" "a read"

conn_scenario "the watcher's own write and a write during multi abort" \
    "a send WATCH|{K};SET|{K}|1;MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K};MULTI;PING" "a read" "b send SET|{K}|2" "b read" \
    "a send EXEC" "a read"

conn_scenario "a write inside the watcher's own transaction does not abort it" \
    "a send WATCH|{K};MULTI;SET|{K}|1;INCR|{K};EXEC" "a read"

conn_scenario "another client's exec aborts" \
    "a send WATCH|{K}" "a read" "b send MULTI;SET|{K}|1;EXEC" "b read" \
    "a send MULTI;PING;EXEC" "a read"

# SET to the same value, LMOVE (both ends), and an XTRIM that removes.
conn_scenario "every write that changes a key aborts" \
    "b send SET|{K}|v;RPUSH|{K2}|x;XADD|{K3}|1-1|f|v;XADD|{K3}|2-1|f|v" "b read" \
    "a send WATCH|{K}" "a read" "c send WATCH|{K2}" "c read" \
    "d send WATCH|{K3}" "d read" "e send WATCH|{K}d" "e read" \
    "b send SET|{K}|v;LMOVE|{K2}|{K}d|LEFT|RIGHT;XTRIM|{K3}|MAXLEN|1" "b read" \
    "a send MULTI;PING;EXEC" "a read" "c send MULTI;PING;EXEC" "c read" \
    "d send MULTI;PING;EXEC" "d read" "e send MULTI;PING;EXEC" "e read"

conn_scenario "creating then deleting, incr, xadd and push abort" \
    "a send WATCH|{K}" "a read" "c send WATCH|{K2}" "c read" \
    "d send WATCH|{K3}" "d read" "e send WATCH|{K}p" "e read" \
    "b send SET|{K}|1;DEL|{K};INCR|{K2};XADD|{K3}|1-1|f|v;RPUSH|{K}p|x" "b read" \
    "a send MULTI;PING;EXEC" "a read" "c send MULTI;PING;EXEC" "c read" \
    "d send MULTI;PING;EXEC" "d read" "e send MULTI;PING;EXEC" "e read"

# Writes that fail or find nothing to do, including LPOP with a count of 0.
conn_scenario "writes that change nothing do not abort" \
    "b send SET|{K}|text;RPUSH|{K2}|x;XADD|{K3}|5-1|f|v" "b read" \
    "a send WATCH|{K}|{K2}|{K3}|{K}m" "a read" \
    "b send DEL|{K}m;LPOP|{K}m;LMOVE|{K}m|{K2}|LEFT|RIGHT;LPOP|{K2}|0" "b read" \
    "b send LPUSH|{K}|x;INCR|{K};SET|{K}|v|EX|0" "b read" \
    "b send XADD|{K3}|1-1|f|v;XDEL|{K3}|9-9;XTRIM|{K3}|MAXLEN|5" "b read" \
    "b send XADD|{K}m|NOMKSTREAM|*|f|v" "b read" \
    "a send MULTI;PING;EXEC" "a read"

conn_scenario "a key that expires after watch aborts, even during multi" \
    "b send SET|{K}|v|PX|100;SET|{K2}|v|PX|100" "b read" \
    "a send WATCH|{K}" "a read" "sleep 0.3" "a send MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K2};MULTI;PING" "a read" "sleep 0.3" "a send EXEC" "a read"

# Already gone at WATCH: reading it is not a change, recreating it is.
conn_scenario "a key already expired at watch counts as absent" \
    "b send SET|{K}|v|PX|50;SET|{K2}|v|PX|50" "b read" "sleep 0.2" \
    "a send WATCH|{K}" "a read" "b send GET|{K}" "b read" \
    "a send MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K2}" "a read" "b send SET|{K2}|new" "b read" \
    "a send MULTI;PING;EXEC" "a read"

conn_scenario "a waiter served by a push changes its destination" \
    "c send BLMOVE|{K}|{K2}|LEFT|RIGHT|0" "sleep 0.2" \
    "a send WATCH|{K2}" "a read" "b send RPUSH|{K}|x" "b read" "c read" \
    "a send MULTI;PING;EXEC" "a read"

# WATCH inside MULTI is refused without failing the transaction; a wrong
# argument count inside MULTI does fail it.
conn_scenario "watch and unwatch arity, and watch inside multi" \
    "a send WATCH;UNWATCH|x;MULTI;WATCH|{K};PING;EXEC" "a read" \
    "a send MULTI;WATCH;EXEC" "a read"

conn_scenario "watches accumulate, and one write aborts every watcher" \
    "a send WATCH|{K}|{K};WATCH|{K};WATCH|{K2}" "a read" \
    "c send WATCH|{K2}" "c read" "b send SET|{K2}|1" "b read" \
    "a send MULTI;PING;EXEC" "a read" "c send MULTI;PING;EXEC" "c read"

conn_scenario "unwatch forgets earlier writes" \
    "a send UNWATCH;WATCH|{K}" "a read" "b send SET|{K}|1" "b read" \
    "a send UNWATCH;MULTI;PING;EXEC" "a read"

conn_scenario "unwatch inside multi is queued and does not stop the abort" \
    "a send WATCH|{K};MULTI;UNWATCH" "a read" "b send SET|{K}|1" "b read" \
    "a send EXEC" "a read"

conn_scenario "exec ends the watch whether it commits or aborts" \
    "a send WATCH|{K};MULTI;EXEC" "a read" "b send SET|{K}|1" "b read" \
    "a send MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K}" "a read" "b send SET|{K}|2" "b read" \
    "a send MULTI;EXEC" "a read" "b send SET|{K}|3" "b read" \
    "a send MULTI;PING;EXEC" "a read"

conn_scenario "discard, execabort and a rejected exec end the watch" \
    "a send WATCH|{K};MULTI;DISCARD" "a read" "b send SET|{K}|1" "b read" \
    "a send MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K};MULTI;NOSUCH;EXEC" "a read" "b send SET|{K}|2" "b read" \
    "a send MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K};MULTI;EXEC|x" "a read" "b send SET|{K}|3" "b read" \
    "a send MULTI;PING;EXEC" "a read" \
    "a send WATCH|{K};EXEC|x" "a read" "b send SET|{K}|4" "b read" \
    "a send MULTI;PING;EXEC" "a read"

conn_scenario "exec and discard without multi keep the watch" \
    "a send WATCH|{K};EXEC;DISCARD" "a read" "b send SET|{K}|1" "b read" \
    "a send MULTI;PING;EXEC" "a read"

conn_scenario "execabort wins over a changed watched key" \
    "a send WATCH|{K}" "a read" "b send SET|{K}|1" "b read" \
    "a send MULTI;NOSUCH;EXEC" "a read"

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