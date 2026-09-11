#!/bin/bash
set -e
cd "$(dirname "$0")/.."
LOG=bench_history.txt
# 快速回归基准：均匀负载（热点负载受环境扰动大，均匀负载稳定可复现）
OUT=$(./bin/bench --nodes 100000 --seconds 5 --clients 8 --warmup 1 --no-redis --zipf 0 --mode optimized 2>&1)
QPS=$(echo "$OUT" | grep -oE "qps [0-9]+" | head -1 | awk '{print $2}')
MEM=$(echo "$OUT" | grep -oE "memory\s+:\s+[0-9.]+" | awk '{print $3}')
NOW=$(date +%Y-%m-%dT%H:%M:%S)
echo "$NOW qps=$QPS mem_mb=$MEM" >> $LOG
LAST=$(tail -2 $LOG | head -1 | grep -oE "qps=[0-9]+" | cut -d= -f2)
if [ -n "$LAST" ] && [ -n "$QPS" ]; then
  DELTA=$(python3 -c "print(f'{(($QPS-$LAST)*100.0/$LAST):+.1f}%')")
  echo "本次 QPS=$QPS vs 上次 $LAST ($DELTA)"
  if python3 -c "exit(0 if $QPS >= $LAST * 0.95 else 1)"; then
    echo "PASS: 性能无退化(>5%)"
  else
    echo "FAIL: QPS 退化超过 5%！请检查最近改动"
    exit 1
  fi
else
  echo "首次基线: QPS=$QPS mem=${MEM}MB"
fi
