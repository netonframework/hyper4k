#!/bin/bash
# 用法: measure <name> <start-cmd> <port>
measure() {
  local name=$1 cmd=$2 port=$3
  pkill -x server 2>/dev/null; pkill -x tier1 2>/dev/null; pkill -x tier2 2>/dev/null; sleep 2
  eval "$cmd" >/tmp/$name.log 2>&1 &
  sleep 3
  curl -sf "http://127.0.0.1:$port/baseline11?a=3&b=4" >/dev/null 2>&1 || curl -sf "http://127.0.0.1:$port/?a=3&b=4" >/dev/null 2>&1
  local pid=$(pgrep -x "${cmd%% *}" | head -1); [ -z "$pid" ] && pid=$(lsof -ti:$port 2>/dev/null | head -1)
  wrk -t4 -c256 -d6s "http://127.0.0.1:$port/baseline11?a=3&b=4" >/dev/null 2>&1
  local c0=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null)
  local out=$(wrk -t4 -c256 -d15s "http://127.0.0.1:$port/baseline11?a=3&b=4" 2>&1)
  local c1=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null)
  local hz=$(getconf CLK_TCK)
  local rps=$(echo "$out" | grep -o 'Requests/sec:.*' | awk '{print $2}')
  local reqs=$(echo "$out" | grep -oE '[0-9]+ requests' | awk '{print $1}')
  local cpu=$(awk "BEGIN{printf \"%.1f\", ($c1-$c0)/$hz}")
  local per=$(awk "BEGIN{printf \"%.1f\", ($c1-$c0)/$hz/$reqs*1e6}")
  echo "$name  rps=$rps  进程CPU=${cpu}s  每请求=${per}µs"
  pkill -x server 2>/dev/null; pkill -x tier1 2>/dev/null; pkill -x tier2 2>/dev/null; sleep 1
}
DS=/data/apps/neton/data/dataset.json
for round in 1 2; do
  echo "--- round $round ---"
  measure tier1 "/data/apps/neton/tier1" 19090
  measure tier2 "/data/apps/neton/tier2" 19091
  measure tier3 "cd /data/apps/neton/prof && NETON_SERVER__PORT=19080 ARENA_H2C_PORT=19082 ARENA_DATASET=$DS /data/apps/neton/prof/server" 19080
done
