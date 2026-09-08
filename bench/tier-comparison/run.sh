#!/bin/bash
# Per-request-cost comparison across tiers, on one box.
#
# Fixes over the first version:
# - server pinned to cores 0,1 and wrk to cores 2,3 (taskset), so the load
#   generator does not steal the server's CPU on a small box;
# - fail-fast on startup; the response body is asserted to be "7" before any
#   measurement, so a broken tier cannot post a number;
# - only the PID this script launched is killed, never by name;
# - raw wrk output is saved per run, and socket errors / non-2xx / timeouts are
#   reported alongside rps and per-request CPU;
# - tiers run in an interleaved, direction-alternating order across rounds.
#
# Still a small VM, not the arena. Differences are localization clues, not
# transferable absolute costs. Read FINDINGS.md before quoting numbers.
set -u
DS=/data/apps/neton/data/dataset.json
URL_SUFFIX="/baseline11?a=3&b=4"
SRV_CPUS=0,1,2
LOAD_CPUS=3
OUT=/tmp/tierbench-out
mkdir -p "$OUT"

launch() {                 # launch <cmd...>  -> echoes the pid
  "$@" >"$OUT/server.log" 2>&1 &
  echo $!
}

measure() {                # measure <name> <port> <cmd...>
  local name=$1 port=$2; shift 2
  pkill -x wrk 2>/dev/null
  local pid; pid=$(launch "$@")
  # Wait for bind + correct answer, or fail.
  local ok="" i
  for i in $(seq 1 20); do
    local body; body=$(curl -sf "http://127.0.0.1:$port$URL_SUFFIX" 2>/dev/null)
    if [ "$body" = "7" ]; then ok=1; break; fi
    kill -0 "$pid" 2>/dev/null || { echo "$name: process died on startup"; cat "$OUT/server.log" | tail -3; return 1; }
    sleep 0.5
  done
  [ -n "$ok" ] || { echo "$name: never answered 7 on $port"; kill "$pid" 2>/dev/null; return 1; }

  # Warm up, then measure with server CPU delta over the window.
  wrk -t4 -c256 -d6s "http://127.0.0.1:$port$URL_SUFFIX" >/dev/null 2>&1
  local c0; c0=$(awk '{print $14+$15}' /proc/$pid/stat)
  wrk -t4 -c256 -d15s "http://127.0.0.1:$port$URL_SUFFIX" >"$OUT/$name.wrk" 2>&1
  local c1; c1=$(awk '{print $14+$15}' /proc/$pid/stat)
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; sleep 1

  local hz rps reqs errs timeouts non2xx cpu per
  hz=$(getconf CLK_TCK)
  rps=$(grep -o 'Requests/sec:.*' "$OUT/$name.wrk" | awk '{print $2}')
  reqs=$(grep -oE '[0-9]+ requests in' "$OUT/$name.wrk" | awk '{print $1}')
  errs=$(grep -o 'Socket errors:.*' "$OUT/$name.wrk" || echo "Socket errors: none")
  non2xx=$(grep -oE 'Non-2xx[^0-9]*[0-9]+' "$OUT/$name.wrk" | grep -oE '[0-9]+$' || echo 0)
  cpu=$(awk "BEGIN{printf \"%.1f\", ($c1-$c0)/$hz}")
  per=$(awk "BEGIN{if($reqs>0) printf \"%.1f\", ($c1-$c0)/$hz/$reqs*1e6; else print \"NA\"}")
  printf '%-8s rps=%-10s cpu=%ss per-req=%sµs  non2xx=%s  %s\n' "$name" "$rps" "$cpu" "$per" "$non2xx" "$errs"
}

t1() { measure tier1 19090 /data/apps/neton/tier1; }
t2() { measure tier2 19091 /data/apps/neton/tier2; }
t25b() { measure tier25b 19092 env HYPER4K_SUM_PORT=19092 /data/apps/neton/tier25b; }
t3() { measure tier3 19080 env NETON_SERVER__PORT=19080 ARENA_H2C_PORT=19082 ARENA_DATASET=$DS /data/apps/neton/prof/server; }

echo "=== round 1 (1,2,2.5B,3) ==="; t1; t2; t25b; t3
echo "=== round 2 (3,2.5B,2,1) ==="; t3; t25b; t2; t1
echo "=== round 3 (2,3,1,2.5B) ==="; t2; t3; t1; t25b
