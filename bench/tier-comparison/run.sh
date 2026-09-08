#!/bin/bash
# The actual experiment, as run on a DEDICATED Fedora box (2 cores, nothing else
# running). No CPU pinning — on a dedicated box it is unnecessary, and on a small
# box taskset starved the GC'd tiers. Load is wrk -t1 -c128. Both Kotlin tiers
# (2.5B and 3) use the same 256 MB GC floor and WARN logging, so their delta is
# not a GC-policy or logging difference. Read FINDINGS.md before quoting numbers.
#
# Per run it records the wrk output per tier per round (never overwritten), the
# server's own CPU delta, request errors, swap page activity and RSS.
set -u
DS=/data/apps/neton/data/dataset.json
SFX="/baseline11?a=3&b=4"
OUT=/tmp/tierbench/$(date +%s); mkdir -p "$OUT"
echo "run dir: $OUT"
{ uname -a; nproc; free -m; for b in tier1 tier2 tier25b tier3; do
    echo "$b sha=$(sha256sum /data/apps/neton/$b 2>/dev/null | cut -c1-16)"; done; } > "$OUT/env.txt"

measure() {  # measure <name> <round> <port> <cmd...>
  local name=$1 round=$2 port=$3; shift 3
  pkill -x wrk 2>/dev/null
  "$@" >"$OUT/$name.r$round.srv" 2>&1 & local pid=$!
  local ok=""
  for i in $(seq 1 24); do
    [ "$(curl -sf "http://127.0.0.1:$port$SFX" 2>/dev/null)" = "7" ] && { ok=1; break; }
    kill -0 $pid 2>/dev/null || { echo "$name.r$round: died on start"; tail -3 "$OUT/$name.r$round.srv"; return 1; }
    sleep 0.5
  done
  [ -n "$ok" ] || { echo "$name.r$round: never answered 7"; kill $pid 2>/dev/null; return 1; }
  wrk -t1 -c128 -d6s "http://127.0.0.1:$port$SFX" >/dev/null 2>&1
  local c0 swi0 c1 swi1 rss hz rps reqs nx errs per
  c0=$(awk '{print $14+$15}' /proc/$pid/stat)
  swi0=$(awk '/pswpin|pswpout/{s+=$2}END{print s}' /proc/vmstat)
  wrk -t1 -c128 -d15s "http://127.0.0.1:$port$SFX" >"$OUT/$name.r$round.wrk" 2>&1
  c1=$(awk '{print $14+$15}' /proc/$pid/stat)
  swi1=$(awk '/pswpin|pswpout/{s+=$2}END{print s}' /proc/vmstat)
  rss=$(awk '/VmRSS/{print $2/1024"MB"}' /proc/$pid/status)
  kill $pid 2>/dev/null; wait $pid 2>/dev/null; sleep 1
  hz=$(getconf CLK_TCK)
  rps=$(grep -o 'Requests/sec:.*' "$OUT/$name.r$round.wrk"|awk '{print $2}')
  reqs=$(grep -oE '[0-9]+ requests in' "$OUT/$name.r$round.wrk"|awk '{print $1}')
  nx=$(grep -oE 'Non-2xx[^0-9]*[0-9]+' "$OUT/$name.r$round.wrk"|grep -oE '[0-9]+$'||echo 0)
  errs=$(grep -o 'Socket errors:.*' "$OUT/$name.r$round.wrk"||echo "no socket errors")
  per=$(awk "BEGIN{if($reqs>0)printf \"%.1f\",($c1-$c0)/$hz/$reqs*1e6;else print \"NA\"}")
  printf '%-8s r%s rps=%-9s per-req=%sus non2xx=%s swap-pages=%s rss=%s | %s\n' \
    "$name" "$round" "$rps" "$per" "$nx" "$((swi1-swi0))" "$rss" "$errs"
}

for r in 1 2 3; do
  case $r in
    1) order="tier1 tier2 tier25b tier3" ;;
    2) order="tier3 tier25b tier2 tier1" ;;
    3) order="tier25b tier1 tier3 tier2" ;;
  esac
  echo "=== round $r ($order) ==="
  for t in $order; do
    case $t in
      tier1)   measure tier1 $r 19090 /data/apps/neton/tier1 ;;
      tier2)   measure tier2 $r 19091 /data/apps/neton/tier2 ;;
      tier25b) measure tier25b $r 19092 env HYPER4K_SUM_PORT=19092 /data/apps/neton/tier25b ;;
      tier3)   measure tier3 $r 19080 env NETON_SERVER__PORT=19080 ARENA_H2C_PORT=19082 ARENA_DATASET=$DS /data/apps/neton/tier3 ;;
    esac
  done
done
echo "raw logs + env in $OUT"
