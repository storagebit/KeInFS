#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2026 Andreas Krause / storagebit
set -euo pipefail

append_optional() {
  local -n argv_ref=$1
  local flag=$2
  local value=${3:-}
  if [[ -n "${value}" && "${value}" != "-" ]]; then
    argv_ref+=("${flag}" "${value}")
  fi
}

: "${KST_BIN:=kst}"
: "${KST_LISTEN:?set KST_LISTEN in the environment file}"
: "${KST_TARGET_ID:?set KST_TARGET_ID in the environment file}"
: "${KST_DRIVE_ID:?set KST_DRIVE_ID in the environment file}"
: "${KST_RAW_DEVICE:?set KST_RAW_DEVICE in the environment file}"

argv=(
  "${KST_BIN}" serve
  --listen "${KST_LISTEN}"
  --listen-backlog "${KST_LISTEN_BACKLOG:-4096}"
  --target-id "${KST_TARGET_ID}"
  --drive-id "${KST_DRIVE_ID}"
  --raw-device "${KST_RAW_DEVICE}"
  --record-mix "${KST_RECORD_MIX:-extent-only}"
  --extent-bytes "${KST_EXTENT_BYTES:-1048576}"
  --packed-bytes "${KST_PACKED_BYTES:-16384}"
  --read-ingress-mode "${KST_READ_INGRESS_MODE:-interrupt}"
  --write-ingress-mode "${KST_WRITE_INGRESS_MODE:-busy}"
  --read-ingress-workers "${KST_READ_INGRESS_WORKERS:-4}"
  --write-ingress-workers "${KST_WRITE_INGRESS_WORKERS:-2}"
  --read-ingress-queue-depth "${KST_READ_INGRESS_QUEUE_DEPTH:-2048}"
  --write-ingress-queue-depth "${KST_WRITE_INGRESS_QUEUE_DEPTH:-2048}"
  --direct-read-mode "${KST_DIRECT_READ_MODE:-busy}"
  --direct-write-mode "${KST_DIRECT_WRITE_MODE:-busy}"
  --direct-read-workers "${KST_DIRECT_READ_WORKERS:-8}"
  --direct-write-workers "${KST_DIRECT_WRITE_WORKERS:-8}"
  --direct-read-queue-depth "${KST_DIRECT_READ_QUEUE_DEPTH:-4096}"
  --direct-write-queue-depth "${KST_DIRECT_WRITE_QUEUE_DEPTH:-4096}"
  --h2-initial-window-bytes "${KST_H2_INITIAL_WINDOW_BYTES:-8388608}"
  --h2-max-concurrent-streams "${KST_H2_MAX_CONCURRENT_STREAMS:-512}"
  --h2-max-send-buffer-bytes "${KST_H2_MAX_SEND_BUFFER_BYTES:-134217728}"
  --stats-root "${KST_STATS_ROOT:-/run/keinfs/kst}"
  --kix-stats-root "${KST_KIX_STATS_ROOT:-/run/keinfs/kix}"
)

append_optional argv --raw-offset-bytes "${KST_RAW_OFFSET_BYTES:-}"
append_optional argv --raw-slice-bytes "${KST_RAW_SLICE_BYTES:-}"
append_optional argv --media-raw-device "${KST_MEDIA_RAW_DEVICE:-}"
append_optional argv --media-raw-offset-bytes "${KST_MEDIA_RAW_OFFSET_BYTES:-}"
append_optional argv --media-raw-slice-bytes "${KST_MEDIA_RAW_SLICE_BYTES:-}"
append_optional argv --key-slots "${KST_KEY_SLOTS:-}"

exec "${argv[@]}"
