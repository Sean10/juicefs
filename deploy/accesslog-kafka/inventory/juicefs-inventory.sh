#!/usr/bin/env bash
# Take a full file inventory of a JuiceFS volume: juicefs dump -> flat records -> file (+ Kafka).
#
# Environment:
#   META_URL          metadata engine URL (required); put the password in META_PASSWORD
#   VOLUME            volume name stored in every record (required)
#   OUT_DIR           output directory (default /var/lib/juicefs-inventory)
#   DUMP_THREADS      juicefs dump --threads (default 10)
#   DUMP_SUBDIR       only inventory this sub-directory (default: whole volume)
#   KEEP_DAYS         delete local inventories older than this (default 35)
#   KAFKA_BOOTSTRAP   if set, also produce every record to Kafka with kcat
#   KAFKA_TOPIC       default juicefs-inventory
#   JUICEFS           juicefs binary (default juicefs)
set -euo pipefail

: "${META_URL:?META_URL is required}"
: "${VOLUME:?VOLUME is required}"
OUT_DIR=${OUT_DIR:-/var/lib/juicefs-inventory}
DUMP_THREADS=${DUMP_THREADS:-10}
DUMP_SUBDIR=${DUMP_SUBDIR:-}
KEEP_DAYS=${KEEP_DAYS:-35}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-}
KAFKA_TOPIC=${KAFKA_TOPIC:-juicefs-inventory}
JUICEFS=${JUICEFS:-juicefs}
HERE=$(cd "$(dirname "$0")" && pwd)

SNAPSHOT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
OUT="$OUT_DIR/$VOLUME/inventory-$(date -u +%Y%m%dT%H%M%SZ).ndjson.gz"
TMP="${OUT%.ndjson.gz}.tmp.ndjson.gz"
mkdir -p "$OUT_DIR/$VOLUME"

dump_args=(--skip-trash --threads "$DUMP_THREADS")
root=/
if [ -n "$DUMP_SUBDIR" ]; then
    dump_args+=(--subdir "$DUMP_SUBDIR")
    root=$DUMP_SUBDIR
fi

# JSON dump is used on purpose: the binary format cannot be streamed by the converter.
"$JUICEFS" dump "$META_URL" "${dump_args[@]}" 2> "$OUT.dump.log" |
    python3 "$HERE/juicefs-inventory.py" --volume "$VOLUME" --root "$root" --snapshot "$SNAPSHOT" -o "$TMP"
mv "$TMP" "$OUT"
echo "inventory written to $OUT"

if [ -n "$KAFKA_BOOTSTRAP" ]; then
    gzip -dc "$OUT" | kcat -P -b "$KAFKA_BOOTSTRAP" -t "$KAFKA_TOPIC" -z lz4 -X linger.ms=100
    echo "inventory produced to kafka topic $KAFKA_TOPIC"
fi

find "$OUT_DIR/$VOLUME" -name 'inventory-*' -mtime +"$KEEP_DAYS" -delete
