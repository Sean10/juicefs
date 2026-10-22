#!/usr/bin/env python3
"""Stream the ".accesslog" of every JuiceFS FUSE mount on this host as JSON lines.

Each output line is one access log record plus context:

  {"mountpoint": "...", "volume": "...", "subdir": "...", "pv": "...", "comm": "...",
   "pod_uid": "...", "pod_namespace": "...", "pod_name": "...", "pvc": "ns/name",
   "line": "<raw access log line>"}

Empty context fields are omitted. Parsing of the line itself is done by Vector
(parse_accesslog.vrl); this program only does what needs host access.

JuiceFS behaviour this relies on (pkg/vfs/accesslog.go, identical in 1.3.x and 1.4.x):
  * records are only produced while some reader keeps ".accesslog" open;
  * each reader has a 10240-line buffer in the client, overflow is silently dropped,
    so reading must never block: when the output is slow we drop and count instead;
  * an idle read returns "#\\n" after one second, so every reader thread wakes up at
    least once per second and can notice a stop request;
  * the pid is the caller's pid in the PID namespace of the juicefs process.

Only the Python 3.6+ standard library is used.
"""

import argparse
import json
import logging
import os
import queue
import re
import signal
import socket
import ssl
import sys
import threading
import time
import urllib.request

log = logging.getLogger("juicefs-accesslog-reader")

HEARTBEAT = b"#"
PID_RE = re.compile(rb"^\S+ \S+ \[uid:\d+,gid:\d+,pid:(\d+)\]")
POD_UID_RE = re.compile(r"pod([0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12})")
CSI_MOUNT_RE = re.compile(r"/([^/]+)-[a-z0-9]{6}$")


def parse_mountinfo(text, prefixes=()):
    """Return [(device, mountpoint)] of fuse.juicefs mounts, de-duplicated by device.

    mountinfo: id parent major:minor root mountpoint opts [optional...] - fstype source superopts
    """
    seen, mounts = set(), []
    for line in text.splitlines():
        f = line.split(" ")
        try:
            sep = f.index("-", 6)
        except ValueError:
            continue
        if f[sep + 1] != "fuse.juicefs":
            continue
        dev = f[2]
        mp = f[4].replace("\\040", " ").replace("\\011", "\t").replace("\\012", "\n").replace("\\134", "\\")
        if prefixes and not any(mp.startswith(p) for p in prefixes):
            continue
        if dev in seen:
            continue
        seen.add(dev)
        mounts.append((dev, mp))
    return mounts


def parse_volume_config(text):
    """Volume name and mount subdir from "<mountpoint>/.config" (json of vfs.Config)."""
    try:
        conf = json.loads(text)
    except ValueError:
        return "", ""
    return (conf.get("Format") or {}).get("Name", ""), (conf.get("Meta") or {}).get("Subdir", "")


def pod_uid_from_cgroup(text):
    """Pod UID from /proc/<pid>/cgroup; handles both cgroupfs and systemd drivers."""
    m = POD_UID_RE.search(text)
    return m.group(1).replace("_", "-") if m else ""


def read_text(path, limit=1 << 20):
    try:
        with open(path, "rb") as f:
            return f.read(limit).decode("utf-8", "replace")
    except OSError:
        return ""


class ProcResolver(object):
    """pid -> (comm, pod_uid), cached; processes that already exited resolve to empty."""

    def __init__(self, proc="/proc", ttl=30, max_entries=100000):
        self.proc, self.ttl, self.max_entries = proc, ttl, max_entries
        self.cache = {}
        self.lock = threading.Lock()

    def resolve(self, pid):
        if not pid:
            return "", ""
        now = time.time()
        with self.lock:
            hit = self.cache.get(pid)
        if hit and now - hit[2] < self.ttl:
            return hit[0], hit[1]
        base = "%s/%d" % (self.proc, pid)
        comm = read_text(base + "/comm").strip()
        uid = pod_uid_from_cgroup(read_text(base + "/cgroup")) if comm else ""
        with self.lock:
            if len(self.cache) >= self.max_entries:
                self.cache.clear()
            self.cache[pid] = (comm, uid, now)
        return comm, uid


class KubeMeta(object):
    """Pod UID -> (namespace, name) for this node and PV -> "ns/pvc", from the in-cluster API."""

    SA = "/var/run/secrets/kubernetes.io/serviceaccount"

    def __init__(self, node, interval=30):
        self.node, self.interval = node, interval
        self.pods, self.pvs = {}, {}
        host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ["KUBERNETES_SERVICE_PORT"]
        if ":" in host:
            host = "[%s]" % host
        self.base = "https://%s:%s" % (host, port)
        self.ctx = ssl.create_default_context(cafile=self.SA + "/ca.crt")

    def get(self, path):
        with open(self.SA + "/token") as f:
            token = f.read().strip()
        req = urllib.request.Request(self.base + path, headers={"Authorization": "Bearer " + token})
        with urllib.request.urlopen(req, context=self.ctx, timeout=20) as r:
            return json.loads(r.read().decode())

    @staticmethod
    def parse_pods(doc):
        return {i["metadata"]["uid"]: (i["metadata"]["namespace"], i["metadata"]["name"])
                for i in doc.get("items", [])}

    @staticmethod
    def parse_pvs(doc):
        out = {}
        for i in doc.get("items", []):
            ref = (i.get("spec") or {}).get("claimRef") or {}
            if ref.get("name"):
                out[i["metadata"]["name"]] = "%s/%s" % (ref.get("namespace", ""), ref["name"])
        return out

    def refresh(self):
        self.pods = self.parse_pods(self.get("/api/v1/pods?fieldSelector=spec.nodeName%3D" + self.node))
        self.pvs = self.parse_pvs(self.get("/api/v1/persistentvolumes"))

    def run(self, stop):
        while not stop.is_set():
            try:
                self.refresh()
            except Exception as e:  # keep the last good maps
                log.warning("refresh kubernetes metadata: %s", e)
            stop.wait(self.interval)


class Output(object):
    """Bounded queue drained by one writer thread; never blocks the readers."""

    def __init__(self, target, maxsize=200000):
        self.target = target
        self.q = queue.Queue(maxsize=maxsize)
        self.dropped = 0
        self.sent = 0
        self.connected = threading.Event()  # set after the first successful connect

    def put(self, data):
        try:
            self.q.put_nowait(data)
        except queue.Full:
            self.dropped += 1

    def _connect(self):
        if self.target == "-":
            return sys.stdout.buffer, None
        kind, _, addr = self.target.partition(":")
        if kind == "unix":
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                s.connect(addr)
            except OSError:
                s.close()
                raise
        elif kind == "tcp":
            host, _, port = addr.rpartition(":")
            s = socket.create_connection((host, int(port)))
        else:
            raise ValueError("unsupported output %r" % self.target)
        return s.makefile("wb", buffering=1 << 16), s

    def run(self, stop):
        backoff = 1
        while not stop.is_set():
            try:
                w, s = self._connect()
            except OSError as e:
                log.warning("connect %s: %s (dropping records until connected)", self.target, e)
                self._drain(stop, backoff)
                backoff = min(backoff * 2, 30)
                continue
            backoff = 1
            self.connected.set()
            log.info("output connected: %s", self.target)
            batch = []
            try:
                while not stop.is_set():
                    try:
                        batch = [self.q.get(timeout=0.5)]
                    except queue.Empty:
                        continue
                    while len(batch) < 1000:
                        try:
                            batch.append(self.q.get_nowait())
                        except queue.Empty:
                            break
                    w.write(b"".join(batch))
                    w.flush()
                    self.sent += len(batch)
                    batch = []
            except (OSError, ValueError) as e:
                self.dropped += len(batch)
                log.warning("write %s: %s", self.target, e)
            finally:
                if s is not None:
                    s.close()
            if self.target == "-":
                stop.set()  # stdout closed: the parent (Vector exec) is gone

    def _drain(self, stop, seconds):
        deadline = time.time() + seconds
        while time.time() < deadline and not stop.is_set():
            try:
                self.q.get(timeout=0.2)
                self.dropped += 1
            except queue.Empty:
                pass


class MountReader(threading.Thread):
    def __init__(self, mountpoint, out, procs, kube=None):
        super(MountReader, self).__init__(name="reader:" + mountpoint, daemon=True)
        self.mountpoint, self.out, self.procs, self.kube = mountpoint, out, procs, kube
        self.stopping = threading.Event()
        self.lines = 0
        volume, subdir = parse_volume_config(read_text(os.path.join(mountpoint, ".config")))
        m = CSI_MOUNT_RE.search(mountpoint)
        self.base = {"mountpoint": mountpoint, "volume": volume, "subdir": subdir, "pv": m.group(1) if m else ""}

    def enrich(self, raw):
        rec = dict(self.base)
        m = PID_RE.match(raw)
        pid = int(m.group(1)) if m else 0
        comm, uid = self.procs.resolve(pid)
        rec["comm"], rec["pod_uid"] = comm, uid
        if self.kube is not None:
            rec["pod_namespace"], rec["pod_name"] = self.kube.pods.get(uid, ("", "")) if uid else ("", "")
            rec["pvc"] = self.kube.pvs.get(rec["pv"], "")
        rec["line"] = raw.decode("utf-8", "surrogateescape")
        rec = {k: v for k, v in rec.items() if v}
        return (json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8", "surrogateescape")

    def run(self):
        path = os.path.join(self.mountpoint, ".accesslog")
        try:
            # unbuffered: each read returns whatever the client has (at most ~1s wait)
            f = open(path, "rb", buffering=0)
        except OSError as e:
            log.warning("open %s: %s", path, e)
            return
        log.info("reading %s", path)
        pending = b""
        try:
            while not self.stopping.is_set():
                chunk = f.read(1 << 17)
                if not chunk:
                    break
                lines = (pending + chunk).split(b"\n")
                pending = lines.pop()
                for raw in lines:
                    if raw and raw != HEARTBEAT:
                        self.lines += 1
                        self.out.put(self.enrich(raw))
        except OSError as e:
            log.info("read %s: %s", path, e)
        finally:
            f.close()  # releases the mount so that a lazy umount can complete
        log.info("stopped reading %s", path)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    env = os.environ.get
    ap.add_argument("--output", default=env("READER_OUTPUT", "-"),
                    help="- (stdout), unix:/path/to.sock or tcp:host:port (env READER_OUTPUT)")
    ap.add_argument("--mount-prefixes", default=env("MOUNT_PREFIXES", ""),
                    help="space separated mount point prefixes to accept, default all (env MOUNT_PREFIXES)")
    ap.add_argument("--rescan-interval", type=float, default=float(env("RESCAN_INTERVAL", "10")))
    ap.add_argument("--mountinfo", default=env("MOUNTINFO", "/proc/self/mountinfo"))
    ap.add_argument("--host-proc", default=env("HOST_PROC", "/proc"),
                    help="procfs of the PID namespace the juicefs processes log pids in (env HOST_PROC)")
    ap.add_argument("--kube", choices=["auto", "on", "off"], default=env("KUBE_META", "auto"),
                    help="resolve pod/PVC names via the in-cluster API (needs NODE_NAME), default auto")
    ap.add_argument("--stats-interval", type=float, default=60)
    ap.add_argument("--log-file", default=env("READER_LOG_FILE", ""),
                    help="log here instead of stderr (Vector's exec source discards stderr)")
    args = ap.parse_args()
    dest = {"filename": args.log_file} if args.log_file else {"stream": sys.stderr}
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", **dest)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    if args.output == "-":
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # exit with the parent; sockets reconnect instead

    out = Output(args.output)
    threading.Thread(target=out.run, args=(stop,), name="output", daemon=True).start()
    procs = ProcResolver(args.host_proc)
    kube = None
    in_cluster = "KUBERNETES_SERVICE_HOST" in os.environ and env("NODE_NAME")
    if args.kube == "on" or (args.kube == "auto" and in_cluster):
        kube = KubeMeta(env("NODE_NAME", ""))
        threading.Thread(target=kube.run, args=(stop,), name="kube", daemon=True).start()

    # Opening ".accesslog" makes the client start formatting records, so wait for the consumer.
    while not stop.is_set() and not out.connected.wait(1):
        pass

    prefixes = tuple(p for p in args.mount_prefixes.split() if p)
    readers = {}
    last_stats = time.time()
    while not stop.is_set():
        mounts = [mp for _, mp in parse_mountinfo(read_text(args.mountinfo, 16 << 20), prefixes)
                  if os.path.exists(os.path.join(mp, ".accesslog"))]
        for mp in mounts:
            r = readers.get(mp)
            if r is None or not r.is_alive():
                r = readers[mp] = MountReader(mp, out, procs, kube)
                r.start()
        for mp in [mp for mp in readers if mp not in mounts]:
            readers.pop(mp).stopping.set()
            log.info("mount %s is gone", mp)
        if time.time() - last_stats >= args.stats_interval:
            last_stats = time.time()
            log.info("stats: mounts=%d lines=%d sent=%d dropped=%d queued=%d", len(readers),
                     sum(r.lines for r in readers.values()), out.sent, out.dropped, out.q.qsize())
        stop.wait(args.rescan_interval)
    for r in readers.values():
        r.stopping.set()
    for r in readers.values():
        r.join(timeout=2)


if __name__ == "__main__":
    main()
