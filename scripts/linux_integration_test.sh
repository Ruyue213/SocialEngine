#!/usr/bin/env bash
# Linux epoll 集成验证一键脚本(在 Linux 主机/虚拟机/容器内执行)。
# 用途:闭环 docs/PERF.md 验证矩阵中「Linux epoll 集成回归」待办项。
# 依赖:bash, make, g++/clang++, curl;可选 socat(半包测试)。
#
#   chmod +x scripts/linux_integration_test.sh
#   ./scripts/linux_integration_test.sh [--asan]
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-18099}
NODES=${NODES:-100000}
LOG=/tmp/dge_linux_it.log
ASAN_FLAG="${1:-}"
ASAN_BUILD=""
EXTRA=""

if [ "$ASAN_FLAG" = "--asan" ]; then
  echo "=== [0/8] ASAN/UBSAN 构建 ==="
  rm -rf build_asan bin_asan
  make -j"$(nproc)" CXXFLAGS="-std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Isrc -Ithird_party/hiredis -MMD -MP" BIN=bin_asan BUILD=build_asan
  ASAN_BUILD="bin_asan"
  EXTRA="ASAN_OPTIONS=detect_leaks=1"
else
  echo "=== [0/8] 常规构建 ==="
  make -j"$(nproc)"
fi

BIN="${ASAN_BUILD:-bin}"

echo "=== [1/8] 单元+回归测试 ==="
$EXTRA "./$BIN/tests" | tail -2

echo "=== [2/8] epoll 语义单测(载荷/ET排空/Mod/WakeFd/fd身份) ==="
$EXTRA "./$BIN/epoll_smoke"

echo "=== [3/8] HTTP 解析模糊 ==="
make fuzz 2>/dev/null | tail -1 || ./bin/parse_hammer | tail -1

echo "=== [4/8] 启动 server(epoll 路径) ==="
pkill -f "dge_it_server" 2>/dev/null || true
cp "./$BIN/server" /tmp/dge_it_server 2>/dev/null || true
SRV="./$BIN/server"
[ -x /tmp/dge_it_server ] && SRV=/tmp/dge_it_server
$EXTRA "$SRV" --nodes "$NODES" --port "$PORT" --no-redis > "$LOG" 2>&1 &
SRV_PID=$!
trap 'kill -TERM $SRV_PID 2>/dev/null || true' EXIT

for i in $(seq 1 60); do
  if curl -sf -m 2 "http://127.0.0.1:$PORT/healthz" > /dev/null 2>&1; then break; fi
  sleep 1
done
curl -sf -m 2 "http://127.0.0.1:$PORT/healthz" > /dev/null || { echo "server 未就绪"; tail -20 "$LOG"; exit 1; }
echo "server ready (pid=$SRV_PID)"

echo "=== [5/8] 基本查询 + keep-alive 管线(curl 单连接多请求) ==="
curl -sf "http://127.0.0.1:$PORT/query" -d '{"type":"dist","a":1,"b":500}' \
  "http://127.0.0.1:$PORT/query" -d '{"type":"cf","a":1,"b":600}' \
  "http://127.0.0.1:$PORT/query" -d '{"type":"sd","a":2}' | head -3

echo ""
echo "=== [6/8] 慢客户端(限速读,触发部分写/CompactSentPrefix) ==="
curl -sf --limit-rate 1k -m 30 "http://127.0.0.1:$PORT/query" -d '{"type":"sd","a":7}' -o /tmp/dge_slow_resp.json
python3 - <<'EOF' || { echo "慢客户端响应校验失败"; exit 1; }
import json
d = json.load(open('/tmp/dge_slow_resp.json'))
assert 'friends' in d, d
print(f"slow-read response ok, {len(d['friends'])} friends")
EOF

echo "=== [7/8] 短连接风暴(fd 复用 + 代际校验) ==="
for i in $(seq 1 200); do
  curl -sf -m 5 "http://127.0.0.1:$PORT/query" -d '{"type":"dist","a":1,"b":9}' > /dev/null
done
echo "200 short-lived connections ok"

echo "=== [8/8] 超时断连 + 优雅停机 ==="
# 客户端提前断开:服务端请求仍在执行,连接关闭不应崩溃
curl -s -m 0.05 "http://127.0.0.1:$PORT/query" -d '{"type":"sd","a":1}' > /dev/null 2>&1 || true
sleep 1
kill -TERM $SRV_PID
for i in $(seq 1 20); do
  kill -0 $SRV_PID 2>/dev/null || break
  sleep 0.5
done
if kill -0 $SRV_PID 2>/dev/null; then
  echo "FAIL: server 未在 10s 内优雅退出"
  kill -KILL $SRV_PID || true
  exit 1
fi
grep -q "exited cleanly" "$LOG" || { echo "FAIL: 缺少干净退出日志"; tail -5 "$LOG"; exit 1; }
echo "graceful shutdown ok"

echo ""
echo "=== Linux epoll 集成验证:全部通过 ==="
echo "结果请回填 docs/PERF.md 验证矩阵(Linux epoll 行)。"
