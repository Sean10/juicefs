#!/usr/bin/env python3
"""Example: join JuiceFS access log events (from Kafka) with a file inventory.

    kcat -b KAFKA -t juicefs-accesslog -C -o beginning -e -q > access.ndjson
    analyze.py --accesslog access.ndjson --inventory inventory-*.ndjson.gz

Read/write events only carry inodes; paths are resolved from the inventory keyed by
(volume, inode), and from create/mkdir/lookup events for files newer than the inventory.
This loads everything in memory and is meant as a reference for the join logic; use
ClickHouse / Spark / DuckDB for real data volumes.
"""

import argparse
import collections
import gzip
import json
import sys


def open_any(path):
    if path == "-":
        return sys.stdin
    return gzip.open(path, "rt", encoding="utf-8") if path.endswith(".gz") else open(path, encoding="utf-8")


def human(n):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if abs(n) < 1024 or unit == "TiB":
            return "%.1f%s" % (n, unit) if unit != "B" else "%d%s" % (n, unit)
        n /= 1024.0


def pct(values, p):
    if not values:
        return 0
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p))]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--accesslog", required=True)
    ap.add_argument("--inventory", nargs="+", default=[])
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--dir-depth", type=int, default=2, help="directory depth for the per-directory report")
    args = ap.parse_args()

    inv = {}
    for f in args.inventory:
        for line in open_any(f):
            r = json.loads(line)
            inv[(r["volume"], r["inode"])] = r

    names = {}  # (volume, inode) -> (parent_inode, name, subdir) learned from the log
    events = []
    for line in open_any(args.accesslog):
        e = json.loads(line)
        events.append(e)
        if e.get("ok") and e.get("inode") and e.get("name") and e.get("op") in ("create", "mkdir", "mknod", "lookup", "symlink", "link"):
            names[(e.get("volume"), e["inode"])] = (e.get("parent_inode"), e["name"], e.get("subdir", ""))

    def path_of(vol, ino, subdir="", depth=0):
        if ino == 1:
            return "/" + subdir if subdir else ""
        r = inv.get((vol, ino))
        if r:
            return r["path"]
        n = names.get((vol, ino))
        if n and depth < 64:
            parent = path_of(vol, n[0], n[2], depth + 1)
            if parent is not None:
                return parent.rstrip("/") + "/" + n[1]
        return None

    per_file = collections.defaultdict(lambda: [0, 0, 0])  # read bytes, write bytes, ops
    per_dir = collections.defaultdict(lambda: [0, 0])
    per_who = collections.defaultdict(lambda: [0, 0, 0])
    ops = collections.Counter()
    errors = collections.Counter()
    lat = collections.defaultdict(list)
    dur = collections.Counter()
    touched = set()
    unresolved = 0
    for e in events:
        op = e.get("op")
        ops[op] += e.get("count", 1)
        if not e.get("ok", True):
            errors[(op, e.get("status"))] += 1
        dur[op] += e.get("duration_us", 0)
        if not e.get("aggregated") and "duration_us" in e:
            lat[op].append(e["duration_us"])
        vol, ino = e.get("volume"), e.get("inode")
        if ino:
            touched.add((vol, ino))
        if op not in ("read", "write"):
            continue
        p = path_of(vol, ino, e.get("subdir", ""))
        if p is None:
            unresolved += 1
            p = "<%s inode %s>" % (vol, ino)
        idx = 0 if op == "read" else 1
        b = e.get("bytes", 0)
        per_file[p][idx] += b
        per_file[p][2] += e.get("count", 1)
        d = "/".join(p.split("/")[:args.dir_depth + 1]) or "/"
        per_dir[d][idx] += b
        who = "%s %s" % (e.get("pvc") or e.get("mountpoint"), e.get("pod_name") or e.get("comm") or "uid:%s" % e.get("uid"))
        per_who[who][idx] += b
        per_who[who][2] += e.get("count", 1)

    print("== %d events, %d inventory records, %d read/write events with unresolved path" % (len(events), len(inv), unresolved))
    print("\n== ops: requests, average latency, p99 latency (only for non-aggregated records)")
    for op, n in ops.most_common():
        p99 = "%6dus" % pct(lat[op], 0.99) if lat[op] else "     -"
        print("  %-16s %8d   avg %8.1fus  p99 %s" % (op, n, dur[op] / n if n else 0, p99))
    if errors:
        print("\n== errors")
        for (op, st), n in errors.most_common(args.top):
            print("  %-10s %-32s %d" % (op, st, n))
    print("\n== top files by read bytes")
    for p, v in sorted(per_file.items(), key=lambda x: -x[1][0])[:args.top]:
        if v[0]:
            print("  %10s  %s" % (human(v[0]), p))
    print("\n== top files by write bytes")
    for p, v in sorted(per_file.items(), key=lambda x: -x[1][1])[:args.top]:
        if v[1]:
            print("  %10s  %s" % (human(v[1]), p))
    print("\n== directories (depth %d): read / write" % args.dir_depth)
    for d, v in sorted(per_dir.items(), key=lambda x: -(x[1][0] + x[1][1]))[:args.top]:
        print("  %10s %10s  %s" % (human(v[0]), human(v[1]), d))
    print("\n== who (pvc/mount, pod/process): read / write / requests")
    for w, v in sorted(per_who.items(), key=lambda x: -(x[1][0] + x[1][1]))[:args.top]:
        print("  %10s %10s %8d  %s" % (human(v[0]), human(v[1]), v[2], w))
    if inv:
        cold = collections.defaultdict(lambda: [0, 0])
        files = 0
        for k, r in inv.items():
            if r["type"] != "regular":
                continue
            files += 1
            if k not in touched:
                d = "/".join(r["path"].split("/")[:args.dir_depth + 1])
                cold[d][0] += 1
                cold[d][1] += r["size"]
        ncold = sum(v[0] for v in cold.values())
        print("\n== cold data: %d of %d inventoried files (%s) not accessed in this log window"
              % (ncold, files, human(sum(v[1] for v in cold.values()))))
        for d, v in sorted(cold.items(), key=lambda x: -x[1][1])[:args.top]:
            print("  %8d files %10s  %s" % (v[0], human(v[1]), d))


if __name__ == "__main__":
    main()
