#!/usr/bin/env bash
# Stream the virtual ".accesslog" file of every JuiceFS FUSE mount on this host to stdout.
#
# Output format (one line per access log record):
#   <mountpoint>\t<raw access log line>
#
# - Mount points are discovered from mountinfo (fstype fuse.juicefs). Bind mounts of a
#   sub-directory (e.g. kubelet pod volume dirs in CSI) are skipped because ".accesslog"
#   only exists at the root of a JuiceFS mount; mounts are de-duplicated by device id.
# - New mounts are picked up and dead readers (umount / client restart) are restarted
#   every $RESCAN_INTERVAL seconds.
# - JuiceFS only produces access logs while at least one reader holds ".accesslog" open,
#   and each reader has a 10240-line in-memory buffer: if the reader is too slow the
#   client silently drops lines for that reader. Keep the downstream pipeline non-blocking.
#
# Requires bash, awk, cat and GNU sed (the official timberio/vector *-debian image has them).
#
# Environment:
#   MOUNTINFO         mountinfo file to parse (default /proc/self/mountinfo)
#   MOUNT_PREFIXES    space separated mount point prefixes to accept (default: all)
#   RESCAN_INTERVAL   seconds between rescans (default 10)

set -u

MOUNTINFO=${MOUNTINFO:-/proc/self/mountinfo}
MOUNT_PREFIXES=${MOUNT_PREFIXES:-}
RESCAN_INTERVAL=${RESCAN_INTERVAL:-10}

declare -A READERS

log() { echo "$(date '+%F %T') juicefs-accesslog-reader: $*" >&2; }

cleanup() {
    for pid in "${READERS[@]}"; do kill "$pid" 2>/dev/null; done
    exit 0
}
trap cleanup INT TERM

# mountinfo: id parent major:minor root mountpoint opts ... - fstype source superopts
list_mounts() {
    awk '{
        sep = 0
        for (i = 7; i <= NF; i++) if ($i == "-") { sep = i; break }
        if (sep == 0 || $(sep + 1) != "fuse.juicefs" || $4 != "/") next
        if (seen[$3]++) next
        mp = $5
        gsub(/\\040/, " ", mp)
        print mp
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
    # sed prefixes the mount point and drops the "#" heartbeat JuiceFS emits every second
    # when idle; GNU sed -u is required since mawk/busybox buffer stdin and stall the stream.
    cat "$mp/.accesslog" 2>/dev/null > >(exec sed -u -e '/^#$/d' -e "s|^|${mp//|/\\|}\t|") &
    READERS["$mp"]=$!
    log "started reader for $mp (pid $!)"
}

while true; do
    declare -A alive=()
    while IFS= read -r mp; do
        [ -z "$mp" ] && continue
        accepted "$mp" || continue
        alive["$mp"]=1
        pid=${READERS["$mp"]:-}
        if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
            [ -e "$mp/.accesslog" ] && start_reader "$mp"
        fi
    done < <(list_mounts)
    for mp in "${!READERS[@]}"; do
        if [ -z "${alive[$mp]:-}" ]; then
            kill "${READERS[$mp]}" 2>/dev/null
            unset 'READERS[$mp]'
            log "stopped reader for $mp (unmounted)"
        fi
    done
    unset alive
    sleep "$RESCAN_INTERVAL" &
    wait $!
done
