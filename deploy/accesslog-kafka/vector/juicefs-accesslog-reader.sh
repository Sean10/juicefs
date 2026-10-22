#!/usr/bin/env bash
# Stream the virtual ".accesslog" file of every JuiceFS FUSE mount on this host to stdout.
#
# Output format (one line per access log record, tab separated):
#   <mountpoint> <comm> <pod_uid> <pod_namespace> <pod_name> <pvc> <raw access log line>
#
# - Mount points are discovered from mountinfo (fstype fuse.juicefs), de-duplicated by
#   device id. Only the root of a JuiceFS mount exposes ".accesslog".
# - New mounts are picked up and dead readers (umount / client restart) are restarted
#   every $RESCAN_INTERVAL seconds.
# - JuiceFS only produces access logs while at least one reader holds ".accesslog" open,
#   and each reader has a 10240-line in-memory buffer: if the reader is too slow the
#   client silently drops lines for that reader. Keep the downstream pipeline non-blocking.
# - The pid in the log is the caller's pid in the PID namespace of the juicefs process.
#   It is resolved through $HOST_PROC/<pid> into the process name and, on Kubernetes, the
#   pod UID (from cgroup). Pod / PVC names come from the optional $K8S_META_DIR files.
#   CSI mount pods need hostPID (mountPodPatch) or every pid is 0.
#
# Requires bash, awk, cat and perl (perl-base is part of every Debian/Ubuntu install,
# including the official timberio/vector *-debian image).
#
# Environment:
#   MOUNTINFO         mountinfo file to parse (default /proc/self/mountinfo)
#   MOUNT_PREFIXES    space separated mount point prefixes to accept (default: all)
#   RESCAN_INTERVAL   seconds between rescans (default 10)
#   HOST_PROC         procfs of the host PID namespace (default /proc)
#   K8S_META_DIR      directory with pods.tsv (uid ns name) and pvs.tsv (pv ns pvc),
#                     refreshed by an external process (default: unset, no k8s names)

set -u

MOUNTINFO=${MOUNTINFO:-/proc/self/mountinfo}
MOUNT_PREFIXES=${MOUNT_PREFIXES:-}
RESCAN_INTERVAL=${RESCAN_INTERVAL:-10}
export HOST_PROC=${HOST_PROC:-/proc}
export K8S_META_DIR=${K8S_META_DIR:-}

declare -A READERS

log() { echo "$(date '+%F %T') juicefs-accesslog-reader: $*" >&2; }

cleanup() {
    for pid in "${READERS[@]}"; do kill "$pid" 2>/dev/null; done
    exit 0
}
trap cleanup INT TERM

# Drops the "#" heartbeat JuiceFS emits every second when idle, resolves the caller pid
# and prefixes the context columns. Output is unbuffered so the stream never stalls.
read -r -d '' ENRICH <<'PERL'
$| = 1;
my ($mp) = @ARGV; @ARGV = ();
my $proc = $ENV{HOST_PROC} || "/proc";
my $meta = $ENV{K8S_META_DIR} || "";
my (%pidc, %pods, %pvs, $metaAt);
my $pv = ($mp =~ m{/([^/]+)-[a-z0-9]{6}$}) ? $1 : "";
sub slurp { my $f = shift; open(my $h, "<", $f) or return ""; local $/; my $s = <$h>; close $h; return $s // ""; }
sub loadmeta {
    return if !$meta || (defined $metaAt && time - $metaAt < 30);
    $metaAt = time;
    for my $t (["pods.tsv", \%pods], ["pvs.tsv", \%pvs]) {
        open(my $h, "<", "$meta/$t->[0]") or next;
        my %m;
        while (<$h>) { chomp; my ($k, @v) = split /\t/; $m{$k} = \@v if defined $k; }
        close $h; %{$t->[1]} = %m;
    }
}
sub who {
    my $pid = shift;
    return ("", "") if !$pid;
    my $c = $pidc{$pid};
    return @$c[0, 1] if $c && time - $c->[2] < 30;
    my $comm = slurp("$proc/$pid/comm"); chomp $comm;
    my $uid = (slurp("$proc/$pid/cgroup") =~ /pod([0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12})/) ? $1 : "";
    $uid =~ tr/_/-/;
    %pidc = () if keys %pidc > 100000;
    $pidc{$pid} = [$comm, $uid, time];
    return ($comm, $uid);
}
while (my $l = <STDIN>) {
    next if $l eq "#\n";
    loadmeta();
    my ($pid) = $l =~ /^\S+ \S+ \[uid:\d+,gid:\d+,pid:(\d+)\]/;
    my ($comm, $uid) = who($pid);
    my ($ns, $name) = $uid && $pods{$uid} ? @{$pods{$uid}} : ("", "");
    my $pvc = $pv && $pvs{$pv} ? join("/", @{$pvs{$pv}}) : "";
    $comm =~ tr/\t/ /;
    print join("\t", $mp, $comm, $uid, $ns // "", $name // "", $pvc, $l);
}
PERL

# mountinfo: id parent major:minor root mountpoint opts ... - fstype source superopts
list_mounts() {
    awk '{
        sep = 0
        for (i = 7; i <= NF; i++) if ($i == "-") { sep = i; break }
        if (sep == 0 || $(sep + 1) != "fuse.juicefs") next
        mp = $5
        gsub(/\\040/, " ", mp)
        print $3 "\t" mp
    }' "$MOUNTINFO"
}

accepted() {
    [ -z "$MOUNT_PREFIXES" ] && return 0
    local p
    for p in $MOUNT_PREFIXES; do
        case "$1" in "$p"*) return 0 ;; esac
    done
    return 1
}

start_reader() {
    local mp=$1
    # Track the pid of cat itself: an open ".accesslog" handle keeps the mount busy, so a
    # lazily unmounted client cannot finish exiting until this reader is killed.
    cat "$mp/.accesslog" 2>/dev/null > >(exec perl -e "$ENRICH" "$mp") &
    READERS["$mp"]=$!
    log "started reader for $mp (pid $!)"
}

while true; do
    declare -A alive=() seen=()
    while IFS=$'\t' read -r dev mp; do
        [ -z "$mp" ] && continue
        accepted "$mp" || continue
        [ -n "${seen[$dev]:-}" ] && continue
        [ -e "$mp/.accesslog" ] || continue
        seen["$dev"]=1
        alive["$mp"]=1
        pid=${READERS["$mp"]:-}
        if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
            start_reader "$mp"
        fi
    done < <(list_mounts)
    for mp in "${!READERS[@]}"; do
        if [ -z "${alive[$mp]:-}" ]; then
            kill "${READERS[$mp]}" 2>/dev/null
            unset 'READERS[$mp]'
            log "stopped reader for $mp (unmounted)"
        fi
    done
    unset alive seen
    sleep "$RESCAN_INTERVAL" &
    wait $!
done
