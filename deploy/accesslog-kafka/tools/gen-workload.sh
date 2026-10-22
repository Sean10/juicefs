#!/usr/bin/env bash
# Generate a mixed JuiceFS workload so that every common access log op shows up.
# Usage: gen-workload.sh <dir-inside-juicefs> [rounds]
set -eu
BASE=${1:?usage: $0 <dir-inside-juicefs> [rounds]}
ROUNDS=${2:-3}

for r in $(seq 1 "$ROUNDS"); do
    d="$BASE/round-$r"
    mkdir -p "$d/logs" "$d/data/中文 目录" "$d/tmp"
    for i in $(seq 1 20); do echo "line $i $(date +%s%N)" >> "$d/logs/app.log"; done
    dd if=/dev/urandom of="$d/data/blob-$r.bin" bs=1M count=8 status=none
    dd if="$d/data/blob-$r.bin" of=/dev/null bs=1M status=none
    dd if="$d/data/blob-$r.bin" of=/dev/null bs=4k count=16 skip=500 status=none
    echo "hello" > "$d/data/中文 目录/报告,v1.txt"
    cat "$d/data/中文 目录/报告,v1.txt" > /dev/null
    for i in $(seq 1 30); do : > "$d/tmp/small-$i"; done
    ls -lR "$d" > /dev/null
    find "$d" -type f -print0 | xargs -0 -r stat > /dev/null
    mv "$d/tmp/small-1" "$d/tmp/renamed-1"
    ln -s ../logs/app.log "$d/tmp/link-to-log"
    readlink "$d/tmp/link-to-log" > /dev/null
    ln "$d/logs/app.log" "$d/logs/app.log.hardlink"
    chmod 640 "$d/logs/app.log"
    truncate -s 1M "$d/data/blob-$r.bin"
    setfattr -n user.owner -v team-a "$d/data/blob-$r.bin" 2>/dev/null || true
    getfattr -d "$d/data/blob-$r.bin" > /dev/null 2>&1 || true
    cat "$d/does-not-exist" 2>/dev/null || true
    rmdir "$d/data" 2>/dev/null || true
    rm -f "$d"/tmp/small-1[0-9]
    df "$BASE" > /dev/null
done
echo "workload done: $ROUNDS rounds under $BASE"
