#!/usr/bin/env python3
"""Convert a JuiceFS JSON metadata dump into a flat file inventory (one record per path).

    juicefs dump META-URL --skip-trash --threads 10 | juicefs-inventory.py --volume myjfs -o inv.ndjson.gz

The dump is parsed line by line in constant memory, relying on the layout written by
`juicefs dump` (JSON format, 1.x): every tree entry starts with a `"<name>": {` line followed
by a single-line `"attr": {...}`, indented by two spaces per level. Binary dumps
(`--binary`) are not supported.

Records carry the inode so they can be joined with access log events (which only carry
inodes for read/write). Hard links produce one record per path with the same inode.
"""

import argparse
import csv
import datetime
import gzip
import json
import re
import signal
import sys

ENTRY_RE = re.compile(r'^( +)"(.*)": \{$')
ATTR_PREFIX = '"attr": '
SYMLINK_PREFIX = '"symlink": '
FIELDS = ["volume", "snapshot", "inode", "parent_inode", "path", "name", "depth", "type", "size",
          "mode", "uid", "gid", "nlink", "atime", "mtime", "ctime", "symlink"]


def unescape(s):
    # inverse of escape() in pkg/meta/dump.go: %XX for bytes <= 0x20, '%', '"', '\\' and invalid UTF-8
    if "%" not in s:
        return s
    out = bytearray()
    b = s.encode()
    i = 0
    while i < len(b):
        c = b[i]
        if c == 0x25 and i + 2 < len(b) and re.match(rb"[0-9A-Fa-f]{2}", b[i + 1:i + 3]):
            out.append(int(b[i + 1:i + 3], 16))
            i += 3
            continue
        out.append(c)
        i += 1
    return out.decode("utf-8", "surrogateescape")


def iter_inventory(lines, root_path="/"):
    in_tree = False
    names = []   # names[d-1] is the entry name at depth d (root children have depth 1)
    inodes = [1]  # inodes[d] is the inode of the entry at depth d; root is depth 0
    pending = None
    held = None  # symlink record waiting for its "symlink" line
    for line in lines:
        line = line.rstrip("\n")
        if not in_tree:
            if line == '  "FSTree": {':
                in_tree, pending = True, 0
            continue
        stripped = line.lstrip(" ")
        if held is not None:
            if stripped.startswith(SYMLINK_PREFIX):
                target = unescape(json.loads(stripped[len(SYMLINK_PREFIX):].rstrip(",")))
                held[4]["symlink"] = target
                held[4]["length"] = len(target.encode("utf-8", "surrogateescape"))
            yield tuple(held)
            held = None
        if line.startswith("  }"):
            break
        indent = len(line) - len(stripped)
        if pending is not None and stripped.startswith(ATTR_PREFIX):
            attr = json.loads(stripped[len(ATTR_PREFIX):].rstrip(","))
            depth = pending
            pending = None
            del inodes[depth:]
            inodes.append(attr["inode"])
            if depth == 0:
                continue
            path = root_path.rstrip("/") + "/" + "/".join(names[:depth])
            rec = [depth, inodes[depth - 1], path, names[depth - 1], attr]
            if attr.get("type") == "symlink":
                held = rec
            else:
                yield tuple(rec)
            continue
        m = ENTRY_RE.match(line)
        if m and (indent // 2) % 2 == 1 and indent >= 6:
            depth = (indent // 2 - 1) // 2
            del names[depth - 1:]
            names.append(unescape(m.group(2)))
            pending = depth
    if held is not None:
        yield tuple(held)


EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def ts(sec, nsec):
    t = EPOCH + datetime.timedelta(seconds=sec, microseconds=nsec // 1000)
    return t.isoformat(timespec="microseconds")


def main():
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dump", nargs="?", default="-", help="JSON dump file (.gz supported), default stdin")
    ap.add_argument("-o", "--output", default="-", help="output file (.gz supported), default stdout")
    ap.add_argument("--format", choices=["ndjson", "csv"], default="ndjson")
    ap.add_argument("--volume", required=True, help="volume name stored in every record")
    ap.add_argument("--root", default="/", help="path prefix of the dumped tree (the value of dump --subdir)")
    ap.add_argument("--snapshot", default=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
                    help="snapshot time stored in every record (default now, UTC)")
    ap.add_argument("--files-only", action="store_true", help="skip directories")
    args = ap.parse_args()

    if args.dump == "-":
        src = sys.stdin
    elif args.dump.endswith(".gz"):
        src = gzip.open(args.dump, "rt", encoding="utf-8", errors="surrogateescape")
    else:
        src = open(args.dump, encoding="utf-8", errors="surrogateescape")
    if args.output == "-":
        dst = sys.stdout
    elif args.output.endswith(".gz"):
        dst = gzip.open(args.output, "wt", encoding="utf-8", errors="surrogateescape", compresslevel=6)
    else:
        dst = open(args.output, "w", encoding="utf-8", errors="surrogateescape")

    writer = None
    if args.format == "csv":
        writer = csv.DictWriter(dst, fieldnames=FIELDS)
        writer.writeheader()
    n = total = 0
    for depth, parent, path, name, a in iter_inventory(src, args.root):
        if args.files_only and a["type"] == "directory":
            continue
        rec = {
            "volume": args.volume,
            "snapshot": args.snapshot,
            "inode": a["inode"],
            "parent_inode": parent,
            "path": path,
            "name": name,
            "depth": depth,
            "type": a["type"],
            "size": a.get("length", 0),
            "mode": "%04o" % (a.get("mode", 0) & 0o7777),
            "uid": a.get("uid", 0),
            "gid": a.get("gid", 0),
            "nlink": a.get("nlink", 0),
            "atime": ts(a.get("atime", 0), a.get("atimensec", 0)),
            "mtime": ts(a.get("mtime", 0), a.get("mtimensec", 0)),
            "ctime": ts(a.get("ctime", 0), a.get("ctimensec", 0)),
            "symlink": a.get("symlink"),
        }
        if writer:
            writer.writerow(rec)
        else:
            dst.write(json.dumps(rec, ensure_ascii=False) + "\n")
        n += 1
        total += rec["size"] if rec["type"] == "regular" else 0
    dst.flush()
    if dst is not sys.stdout:
        dst.close()
    print("inventory: %d records, %d bytes in regular files" % (n, total), file=sys.stderr)


if __name__ == "__main__":
    main()
