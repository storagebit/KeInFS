#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2026 Andreas Krause / storagebit
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TEMPLATE_ROOT="${ROOT_DIR}/packaging/templates"

ALIGNMENT_BYTES=4096
GIB=$((1024 * 1024 * 1024))
AUTO_KIX_ARENA_MIN_BYTES=$((16 * GIB))
AUTO_KIX_ARENA_MAX_BYTES=$((256 * GIB))
AUTO_KIX_ARENA_FRACTION_DIVISOR=50
AUTO_KIX_MIN_MEDIA_BYTES=$((8 * GIB))
QUICKSTART_MGMT_NETWORK_NAME="keinfs-cp-mgmt"
QUICKSTART_TARGET_NETWORK_NAME="keinfs-target-fabric"
QUICKSTART_MGMT_BRIDGE_NAME="keinfsmgmt0"
QUICKSTART_TARGET_BRIDGE_NAME="keinfstgt0"
QUICKSTART_CLOUD_IMAGE_URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
QUICKSTART_FDB_CLIENT_DEB_URL="https://github.com/apple/foundationdb/releases/download/7.3.77/foundationdb-clients_7.3.77-1_amd64.deb"
QUICKSTART_FDB_SERVER_DEB_URL="https://github.com/apple/foundationdb/releases/download/7.3.77/foundationdb-server_7.3.77-1_amd64.deb"

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
usage:
  tools/keinfs-tooling.sh render-configs --config-env build/config.env --out-dir build/render/etc/keinfs
  tools/keinfs-tooling.sh render-systemd --config-env build/config.env --out-dir build/render/systemd
  tools/keinfs-tooling.sh render-single-host-vm-lab --config-env build/config.env --out-dir build/quickstart/single-host-vm-lab --device /dev/nvme0n1
EOF
}

load_config_env() {
  local path=$1
  [[ -f "${path}" ]] || die "missing config env ${path}; run ./configure first"
  # shellcheck disable=SC1090
  source "${path}"
}

toml_escape() {
  local value=$1
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  printf '%s' "${value}"
}

toml_string() {
  printf '"%s"' "$(toml_escape "$1")"
}

toml_array() {
  local out="[" item sep=""
  for item in "$@"; do
    out+="${sep}$(toml_string "${item}")"
    sep=", "
  done
  out+="]"
  printf '%s' "${out}"
}

write_file() {
  local path=$1
  local content=$2
  local mode=${3:-0644}
  mkdir -p "$(dirname "${path}")"
  printf '%s' "${content}" >"${path}"
  chmod "${mode}" "${path}"
}

render_template_text() {
  local template_name=$1
  shift
  local template_path="${TEMPLATE_ROOT}/${template_name}"
  [[ -f "${template_path}" ]] || die "missing template ${template_path}"

  local content
  content=$(<"${template_path}")
  while (($# > 0)); do
    local key=$1
    local value=$2
    shift 2
    content=${content//\{\{${key}\}\}/${value}}
  done

  if [[ "${content}" == *'{{'* || "${content}" == *'}}'* ]]; then
    die "unresolved placeholders remain in template ${template_name}"
  fi
  printf '%s' "${content}"
}

render_template() {
  local template_name=$1
  local out_path=$2
  shift 2
  local rendered
  rendered=$(render_template_text "${template_name}" "$@")
  write_file "${out_path}" "${rendered}"
}

service_paths() {
  KMS_STATS_ROOT="${RUNSTATEDIR}/keinfs/kms"
  KAS_STATS_ROOT="${RUNSTATEDIR}/keinfs/kas"
  KRS_STATS_ROOT="${RUNSTATEDIR}/keinfs/krs"
  KST_STATS_ROOT="${RUNSTATEDIR}/keinfs/kst"
  KIX_STATS_ROOT="${RUNSTATEDIR}/keinfs/kix"
}

align_down() {
  local value=$1
  local alignment=${2:-${ALIGNMENT_BYTES}}
  printf '%s' "$((value - (value % alignment)))"
}

auto_kix_arena_slice_bytes() {
  local device_bytes=$1
  local proportional max_allowed desired
  proportional=$(align_down "$((device_bytes / AUTO_KIX_ARENA_FRACTION_DIVISOR))")
  max_allowed=$(align_down "$((device_bytes - AUTO_KIX_MIN_MEDIA_BYTES))")
  (( max_allowed > 0 )) || die "slice only leaves ${device_bytes} bytes total; need more than ${AUTO_KIX_MIN_MEDIA_BYTES} bytes for chunk media"
  desired=${proportional}
  (( desired >= AUTO_KIX_ARENA_MIN_BYTES )) || desired=${AUTO_KIX_ARENA_MIN_BYTES}
  (( desired <= AUTO_KIX_ARENA_MAX_BYTES )) || desired=${AUTO_KIX_ARENA_MAX_BYTES}
  (( desired <= max_allowed )) || desired=${max_allowed}
  (( desired > 0 )) || die "auto arena sizing produced a useless zero-byte KIX arena"
  printf '%s' "${desired}"
}

detect_device_bytes() {
  local device=$1
  if [[ "${device}" == /dev/* ]]; then
    local sysfs_size="/sys/class/block/${device##*/}/size"
    if [[ -r "${sysfs_size}" ]]; then
      local sectors
      read -r sectors <"${sysfs_size}"
      printf '%s' "$((sectors * 512))"
      return
    fi
  fi

  if command -v lsblk >/dev/null 2>&1; then
    local value
    value=$(lsblk -bdno SIZE "${device}" 2>/dev/null | tr -d '[:space:]' || true)
    if [[ "${value}" =~ ^[0-9]+$ ]]; then
      printf '%s' "${value}"
      return
    fi
  fi

  if command -v blockdev >/dev/null 2>&1; then
    local size
    size=$(blockdev --getsize64 "${device}" 2>/dev/null || true)
    if [[ "${size}" =~ ^[0-9]+$ ]]; then
      printf '%s' "${size}"
      return
    fi
    die "failed to read ${device} size via blockdev"
  fi

  die "blockdev is not available; pass --device-bytes explicitly when rendering a quick-start layout on a non-Linux box"
}

quickstart_mac() {
  printf '52:54:00:%02x:00:%02x' "$1" "$2"
}

render_libvirt_network() {
  local out_path=$1
  local name=$2
  local bridge_name=$3
  local gateway_ip=$4
  local dhcp_start=$5
  local dhcp_end=$6
  local forward_mode=$7
  shift 7

  local host_xml="" entry host_name host_mac host_ip
  for entry in "$@"; do
    IFS='|' read -r host_name host_mac host_ip <<<"${entry}"
    host_xml+="      <host mac='${host_mac}' name='${host_name}' ip='${host_ip}'/>"$'\n'
  done

  local forward_xml=""
  if [[ -n "${forward_mode}" ]]; then
    forward_xml+="  <forward mode='${forward_mode}'>"$'\n'
    forward_xml+="    <nat>"$'\n'
    forward_xml+="      <port start='1024' end='65535'/>"$'\n'
    forward_xml+="    </nat>"$'\n'
    forward_xml+="  </forward>"$'\n'
  fi

  local content
  content="<network>
  <name>${name}</name>
${forward_xml}  <bridge name='${bridge_name}' stp='on' delay='0'/>
  <ip address='${gateway_ip}' netmask='255.255.255.0'>
    <dhcp>
      <range start='${dhcp_start}' end='${dhcp_end}'/>
${host_xml}    </dhcp>
  </ip>
</network>
"
  write_file "${out_path}" "${content}"
}

render_configs_command() {
  local config_env="" out_dir=""
  while (($# > 0)); do
    case "$1" in
      --config-env)
        config_env=$2
        shift 2
        ;;
      --out-dir)
        out_dir=$2
        shift 2
        ;;
      *)
        die "unknown option for render-configs: $1"
        ;;
    esac
  done

  [[ -n "${config_env}" && -n "${out_dir}" ]] || die "render-configs requires --config-env and --out-dir"
  load_config_env "${config_env}"
  service_paths

  local kas_loopback kms_loopback context_empty_array
  kas_loopback=$(toml_array "http://127.0.0.1:50061")
  kms_loopback=$(toml_string "http://127.0.0.1:50060")
  context_empty_array=$(toml_array)

  render_template "config/kms.toml.in" "${out_dir}/kms/default.toml" \
    FOUNDATIONDB_CLUSTER_FILE "$(toml_string "${FOUNDATIONDB_CLUSTER_FILE}")" \
    NOTIFICATION_NATS_URL "$(toml_string "${NOTIFICATION_NATS_URL}")" \
    NOTIFICATION_SUBJECT "$(toml_string "${NOTIFICATION_SUBJECT}")" \
    KMS_STATS_ROOT "$(toml_string "${KMS_STATS_ROOT}")" \
    KMS_LISTEN_ADDR "$(toml_string "127.0.0.1:50060")" \
    KMS_SHARD_ID "$(toml_string "kms-shard-0001")" \
    KMS_PUBLIC_ENDPOINT "$(toml_string "http://127.0.0.1:50060")" \
    KMS_REPLICA_ENDPOINTS "${context_empty_array}" \
    KMS_KAS_ENDPOINTS "${kas_loopback}"

  render_template "config/kas.toml.in" "${out_dir}/kas/alloc-shard-00.toml" \
    FOUNDATIONDB_CLUSTER_FILE "$(toml_string "${FOUNDATIONDB_CLUSTER_FILE}")" \
    KAS_STATS_ROOT "$(toml_string "${KAS_STATS_ROOT}")" \
    KAS_LISTEN_ADDR "$(toml_string "127.0.0.1:50061")" \
    KAS_PUBLIC_ENDPOINT "$(toml_string "http://127.0.0.1:50061")" \
    KAS_ALLOCATION_SHARD_ID "$(toml_string "alloc-shard-00")"

  render_template "config/krs.toml.in" "${out_dir}/krs/default.toml" \
    KRS_KMS_ENDPOINT "${kms_loopback}" \
    KRS_KAS_ENDPOINTS "$(toml_string "http://127.0.0.1:50061")" \
    KRS_LEASE_OWNER "$(toml_string "krs-local")" \
    KRS_STATS_ROOT "$(toml_string "${KRS_STATS_ROOT}")"

  render_template "config/keinctl-contexts.toml.in" "${out_dir}/keinctl/contexts.toml" \
    CONTEXT_NAME_KEY "default" \
    CONTEXT_NAME_VALUE "$(toml_string "default")" \
    CONTEXT_LABEL "$(toml_string "default")" \
    CONTEXT_KMS_ENDPOINT "${kms_loopback}" \
    CONTEXT_KAS_ENDPOINT "$(toml_string "http://127.0.0.1:50061")" \
    CONTEXT_KMS_RUNTIME_ROOT "$(toml_string "${KMS_STATS_ROOT}")" \
    CONTEXT_KAS_RUNTIME_ROOT "$(toml_string "${KAS_STATS_ROOT}")" \
    CONTEXT_KRS_RUNTIME_ROOT "$(toml_string "${KRS_STATS_ROOT}")" \
    CONTEXT_KST_RUNTIME_ROOT "$(toml_string "${KST_STATS_ROOT}")" \
    CONTEXT_KST_HTTP_ENDPOINTS "${context_empty_array}"

  render_template "config/kst-target.env.in" "${out_dir}/kst/target-example.env" \
    KST_BIN "${BINDIR}/kst" \
    KST_STATS_ROOT "${KST_STATS_ROOT}" \
    KIX_STATS_ROOT "${KIX_STATS_ROOT}"
}

render_systemd_command() {
  local config_env="" out_dir=""
  while (($# > 0)); do
    case "$1" in
      --config-env)
        config_env=$2
        shift 2
        ;;
      --out-dir)
        out_dir=$2
        shift 2
        ;;
      *)
        die "unknown option for render-systemd: $1"
        ;;
    esac
  done

  [[ -n "${config_env}" && -n "${out_dir}" ]] || die "render-systemd requires --config-env and --out-dir"
  load_config_env "${config_env}"

  local name
  for name in \
    "keinfs-kms@.service" \
    "keinfs-kas@.service" \
    "keinfs-krs@.service" \
    "keinfs-kst@.service"; do
    render_template "systemd/${name}.in" "${out_dir}/${name}" \
      BINDIR "${BINDIR}" \
      LIBEXECDIR "${LIBEXECDIR}" \
      SYSCONFDIR "${SYSCONFDIR}" \
      RUNSTATEDIR "${RUNSTATEDIR}" \
      LOCALSTATEDIR "${LOCALSTATEDIR}" \
      SERVICE_USER "${SERVICE_USER}" \
      SERVICE_GROUP "${SERVICE_GROUP}"
  done
}

render_single_host_vm_lab_command() {
  local config_env="" out_dir="" device="" device_bytes="" reserve_bytes="${GIB}"
  local host_ip="" target_host="192.168.131.1" target_count="12" base_port="18080"
  local server_id="single-host-lab" rack_id="lab-rack-01" context_name="vm-lab"

  while (($# > 0)); do
    case "$1" in
      --config-env)
        config_env=$2
        shift 2
        ;;
      --out-dir)
        out_dir=$2
        shift 2
        ;;
      --device)
        device=$2
        shift 2
        ;;
      --device-bytes)
        device_bytes=$2
        shift 2
        ;;
      --reserve-bytes)
        reserve_bytes=$2
        shift 2
        ;;
      --host-ip)
        host_ip=$2
        shift 2
        ;;
      --target-host)
        target_host=$2
        shift 2
        ;;
      --target-count)
        target_count=$2
        shift 2
        ;;
      --base-port)
        base_port=$2
        shift 2
        ;;
      --server-id)
        server_id=$2
        shift 2
        ;;
      --rack-id)
        rack_id=$2
        shift 2
        ;;
      --context-name)
        context_name=$2
        shift 2
        ;;
      *)
        die "unknown option for render-single-host-vm-lab: $1"
        ;;
    esac
  done

  [[ -n "${config_env}" && -n "${out_dir}" && -n "${device}" && -n "${host_ip}" ]] || die "render-single-host-vm-lab requires --config-env, --out-dir, --device, and --host-ip"

  load_config_env "${config_env}"
  service_paths
  [[ -n "${device_bytes}" ]] || device_bytes=$(detect_device_bytes "${device}")

  rm -rf "${out_dir}"
  mkdir -p "${out_dir}"

  local foundationdb_cluster_contents="keinfslab:keinfslab@192.168.130.11:4500"
  local -a cp_nodes=(
    "cp-01|192.168.130.11|192.168.131.11|$(quickstart_mac $((0x13)) $((0x11)))|$(quickstart_mac $((0x14)) $((0x11)))|kms-shard-0001|alloc-shard-00"
    "cp-02|192.168.130.12|192.168.131.12|$(quickstart_mac $((0x13)) $((0x12)))|$(quickstart_mac $((0x14)) $((0x12)))|kms-shard-0002|alloc-shard-01"
    "cp-03|192.168.130.13|192.168.131.13|$(quickstart_mac $((0x13)) $((0x13)))|$(quickstart_mac $((0x14)) $((0x13)))|kms-shard-0003|alloc-shard-02"
  )
  local -a kas_endpoints=()
  local -a kms_endpoints=()
  local -a mgmt_hosts=()
  local -a target_hosts=()

  local nodes_tsv="# name\tmgmt_ip\ttarget_ip\tmgmt_mac\ttarget_mac\tkms_shard_id\tallocation_shard_id"$'\n'
  local node_entry node_name node_mgmt_ip node_target_ip node_mgmt_mac node_target_mac node_kms_shard node_alloc_shard
  for node_entry in "${cp_nodes[@]}"; do
    IFS='|' read -r node_name node_mgmt_ip node_target_ip node_mgmt_mac node_target_mac node_kms_shard node_alloc_shard <<<"${node_entry}"
    kas_endpoints+=("http://${node_mgmt_ip}:50061")
    kms_endpoints+=("http://${node_mgmt_ip}:50060")
    mgmt_hosts+=("${node_name}|${node_mgmt_mac}|${node_mgmt_ip}")
    target_hosts+=("${node_name}|${node_target_mac}|${node_target_ip}")
    nodes_tsv+="${node_name}"$'\t'"${node_mgmt_ip}"$'\t'"${node_target_ip}"$'\t'"${node_mgmt_mac}"$'\t'"${node_target_mac}"$'\t'"${node_kms_shard}"$'\t'"${node_alloc_shard}"$'\n'
  done
  write_file "${out_dir}/control-plane/nodes.tsv" "${nodes_tsv}"

  local usable slice_bytes arena_bytes media_bytes
  (( target_count > 0 )) || die "target_count must be > 0"
  usable=$(align_down "$((device_bytes - reserve_bytes))")
  (( usable > 0 )) || die "device bytes ${device_bytes} minus reserve ${reserve_bytes} leaves nothing useful"
  slice_bytes=$(align_down "$((usable / target_count))")
  (( slice_bytes > 0 )) || die "target slice size would be zero; either shrink target count or use a larger device"
  arena_bytes=$(auto_kix_arena_slice_bytes "${slice_bytes}")
  media_bytes=$(align_down "$((slice_bytes - arena_bytes))")
  (( media_bytes >= AUTO_KIX_MIN_MEDIA_BYTES )) || die "per-target media slice would only be ${media_bytes} bytes; need at least ${AUTO_KIX_MIN_MEDIA_BYTES}. Increase device size or reduce target count."

  local target_rows="# target_id\tport\tdrive_id\traw_device\traw_offset_bytes\traw_slice_bytes\tmedia_raw_device\tmedia_raw_offset_bytes\tmedia_raw_slice_bytes\tkey_slots"$'\n'
  local kst_base_env
  kst_base_env=$(render_template_text "config/kst-target.env.in" \
    KST_BIN "${BINDIR}/kst" \
    KST_STATS_ROOT "${KST_STATS_ROOT}" \
    KIX_STATS_ROOT "${KIX_STATS_ROOT}")

  local index base_offset media_offset raw_offset target_id port drive_id rendered_env
  for ((index = 0; index < target_count; index++)); do
    base_offset=$((index * slice_bytes))
    media_offset=${base_offset}
    raw_offset=$((base_offset + media_bytes))
    printf -v target_id 'epyc-target-%02d' "${index}"
    port=$((base_port + index))
    drive_id=${index}

    target_rows+="${target_id}"$'\t'"${port}"$'\t'"${drive_id}"$'\t'"${device}"$'\t'"${raw_offset}"$'\t'"${arena_bytes}"$'\t'"${device}"$'\t'"${media_offset}"$'\t'"${media_bytes}"$'\t'$'\n'

    rendered_env="${kst_base_env}"$'\n'
    rendered_env+="KST_LISTEN=${target_host}:${port}"$'\n'
    rendered_env+="KST_TARGET_ID=${target_id}"$'\n'
    rendered_env+="KST_DRIVE_ID=${drive_id}"$'\n'
    rendered_env+="KST_RAW_DEVICE=${device}"$'\n'
    rendered_env+="KST_RAW_OFFSET_BYTES=${raw_offset}"$'\n'
    rendered_env+="KST_RAW_SLICE_BYTES=${arena_bytes}"$'\n'
    rendered_env+="KST_MEDIA_RAW_DEVICE=${device}"$'\n'
    rendered_env+="KST_MEDIA_RAW_OFFSET_BYTES=${media_offset}"$'\n'
    rendered_env+="KST_MEDIA_RAW_SLICE_BYTES=${media_bytes}"$'\n'
    write_file "${out_dir}/host/kst/env/${target_id}.env" "${rendered_env}"
  done
  write_file "${out_dir}/host/kst/targets.tsv" "${target_rows}"

  local kas_endpoints_toml kms_replicas_toml other_endpoints
  kas_endpoints_toml=$(toml_array "${kas_endpoints[@]}")
  local kms_nats_url="nats://192.168.130.11:4222"

  for node_entry in "${cp_nodes[@]}"; do
    IFS='|' read -r node_name node_mgmt_ip node_target_ip node_mgmt_mac node_target_mac node_kms_shard node_alloc_shard <<<"${node_entry}"
    local -a replica_endpoints=()
    local other_node
    for other_node in "${cp_nodes[@]}"; do
      local other_name other_mgmt_ip
      IFS='|' read -r other_name other_mgmt_ip _ <<<"${other_node}"
      if [[ "${other_name}" != "${node_name}" ]]; then
        replica_endpoints+=("http://${other_mgmt_ip}:50060")
      fi
    done
    kms_replicas_toml=$(toml_array "${replica_endpoints[@]}")

    render_template "config/kms.toml.in" "${out_dir}/control-plane/${node_name}/kms.toml" \
      FOUNDATIONDB_CLUSTER_FILE "$(toml_string "${FOUNDATIONDB_CLUSTER_FILE}")" \
      NOTIFICATION_NATS_URL "$(toml_string "${kms_nats_url}")" \
      NOTIFICATION_SUBJECT "$(toml_string "${NOTIFICATION_SUBJECT}")" \
      KMS_STATS_ROOT "$(toml_string "${KMS_STATS_ROOT}")" \
      KMS_LISTEN_ADDR "$(toml_string "0.0.0.0:50060")" \
      KMS_SHARD_ID "$(toml_string "${node_kms_shard}")" \
      KMS_PUBLIC_ENDPOINT "$(toml_string "http://${node_mgmt_ip}:50060")" \
      KMS_REPLICA_ENDPOINTS "${kms_replicas_toml}" \
      KMS_KAS_ENDPOINTS "${kas_endpoints_toml}"

    render_template "config/kas.toml.in" "${out_dir}/control-plane/${node_name}/kas.toml" \
      FOUNDATIONDB_CLUSTER_FILE "$(toml_string "${FOUNDATIONDB_CLUSTER_FILE}")" \
      KAS_STATS_ROOT "$(toml_string "${KAS_STATS_ROOT}")" \
      KAS_LISTEN_ADDR "$(toml_string "0.0.0.0:50061")" \
      KAS_PUBLIC_ENDPOINT "$(toml_string "http://${node_mgmt_ip}:50061")" \
      KAS_ALLOCATION_SHARD_ID "$(toml_string "${node_alloc_shard}")"

    write_file "${out_dir}/control-plane/${node_name}/fdb.cluster" "${foundationdb_cluster_contents}"$'\n'
  done

  local kas_endpoints_csv old_ifs
  old_ifs=${IFS}
  IFS=,
  kas_endpoints_csv="${kas_endpoints[*]}"
  IFS=${old_ifs}
  render_template "config/krs.toml.in" "${out_dir}/host/krs/default.toml" \
    KRS_KMS_ENDPOINT "$(toml_string "${kms_endpoints[0]}")" \
    KRS_KAS_ENDPOINTS "$(toml_string "${kas_endpoints_csv}")" \
    KRS_LEASE_OWNER "$(toml_string "krs-${server_id}")" \
    KRS_STATS_ROOT "$(toml_string "${KRS_STATS_ROOT}")"

  local -a target_http_endpoints=()
  for ((index = 0; index < target_count; index++)); do
    target_http_endpoints+=("http://${target_host}:$((base_port + index))")
  done
  render_template "config/keinctl-contexts.toml.in" "${out_dir}/host/keinctl/contexts.toml" \
    CONTEXT_NAME_KEY "${context_name}" \
    CONTEXT_NAME_VALUE "$(toml_string "${context_name}")" \
    CONTEXT_LABEL "$(toml_string "single-host vm lab on ${host_ip}")" \
    CONTEXT_KMS_ENDPOINT "$(toml_string "${kms_endpoints[0]}")" \
    CONTEXT_KAS_ENDPOINT "$(toml_string "${kas_endpoints[0]}")" \
    CONTEXT_KMS_RUNTIME_ROOT "$(toml_string "${KMS_STATS_ROOT}")" \
    CONTEXT_KAS_RUNTIME_ROOT "$(toml_string "${KAS_STATS_ROOT}")" \
    CONTEXT_KRS_RUNTIME_ROOT "$(toml_string "${KRS_STATS_ROOT}")" \
    CONTEXT_KST_RUNTIME_ROOT "$(toml_string "${KST_STATS_ROOT}")" \
    CONTEXT_KST_HTTP_ENDPOINTS "$(toml_array "${target_http_endpoints[@]}")"

  write_file "${out_dir}/host/register-targets.env" \
"KEINCTL_CONTEXT=${context_name}
TARGET_HOST=${target_host}
SERVER_ID=${server_id}
RACK_ID=${rack_id}
KST_RUNTIME_ROOT=${KST_STATS_ROOT}
"

  render_libvirt_network "${out_dir}/libvirt/networks/${QUICKSTART_MGMT_NETWORK_NAME}.xml" \
    "${QUICKSTART_MGMT_NETWORK_NAME}" \
    "${QUICKSTART_MGMT_BRIDGE_NAME}" \
    "192.168.130.1" \
    "192.168.130.100" \
    "192.168.130.200" \
    "nat" \
    "${mgmt_hosts[@]}"

  render_libvirt_network "${out_dir}/libvirt/networks/${QUICKSTART_TARGET_NETWORK_NAME}.xml" \
    "${QUICKSTART_TARGET_NETWORK_NAME}" \
    "${QUICKSTART_TARGET_BRIDGE_NAME}" \
    "${target_host}" \
    "192.168.131.100" \
    "192.168.131.200" \
    "" \
    "${target_hosts[@]}"

  local quickstart_env_path="${out_dir}/quickstart.env"
  {
    printf '# Generated by tools/keinfs-tooling.sh. Source from Bash, do not edit by hand.\n'
    printf 'QUICKSTART_CONTEXT_NAME=%q\n' "${context_name}"
    printf 'QUICKSTART_HOST_IP=%q\n' "${host_ip}"
    printf 'QUICKSTART_TARGET_HOST=%q\n' "${target_host}"
    printf 'QUICKSTART_SERVER_ID=%q\n' "${server_id}"
    printf 'QUICKSTART_RACK_ID=%q\n' "${rack_id}"
    printf 'QUICKSTART_DEVICE=%q\n' "${device}"
    printf 'QUICKSTART_DEVICE_BYTES=%q\n' "${device_bytes}"
    printf 'QUICKSTART_DEVICE_RESERVE_BYTES=%q\n' "${reserve_bytes}"
    printf 'QUICKSTART_TARGET_COUNT=%q\n' "${target_count}"
    printf 'QUICKSTART_TARGET_BASE_PORT=%q\n' "${base_port}"
    printf 'QUICKSTART_TARGET_SLICE_BYTES=%q\n' "${slice_bytes}"
    printf 'QUICKSTART_MGMT_NETWORK_NAME=%q\n' "${QUICKSTART_MGMT_NETWORK_NAME}"
    printf 'QUICKSTART_TARGET_NETWORK_NAME=%q\n' "${QUICKSTART_TARGET_NETWORK_NAME}"
    printf 'QUICKSTART_MGMT_BRIDGE_NAME=%q\n' "${QUICKSTART_MGMT_BRIDGE_NAME}"
    printf 'QUICKSTART_TARGET_BRIDGE_NAME=%q\n' "${QUICKSTART_TARGET_BRIDGE_NAME}"
    printf 'QUICKSTART_VM_MEMORY_MIB=%q\n' "4096"
    printf 'QUICKSTART_VM_VCPUS=%q\n' "2"
    printf 'QUICKSTART_VM_DISK_GIB=%q\n' "32"
    printf 'QUICKSTART_CLOUD_IMAGE_URL=%q\n' "${QUICKSTART_CLOUD_IMAGE_URL}"
    printf 'QUICKSTART_FOUNDATIONDB_CLUSTER_FILE_PATH=%q\n' "${FOUNDATIONDB_CLUSTER_FILE}"
    printf 'QUICKSTART_FOUNDATIONDB_CLUSTER_CONTENTS=%q\n' "${foundationdb_cluster_contents}"
    printf 'QUICKSTART_FDB_CLIENT_DEB_URL=%q\n' "${QUICKSTART_FDB_CLIENT_DEB_URL}"
    printf 'QUICKSTART_FDB_SERVER_DEB_URL=%q\n' "${QUICKSTART_FDB_SERVER_DEB_URL}"
    printf 'QUICKSTART_NATS_URL=%q\n' "${kms_nats_url}"
    (IFS=,; printf 'QUICKSTART_KMS_ENDPOINTS=%q\n' "${kms_endpoints[*]}")
    (IFS=,; printf 'QUICKSTART_KAS_ENDPOINTS=%q\n' "${kas_endpoints[*]}")
  } >"${quickstart_env_path}"

  write_file "${out_dir}/README.txt" \
"Single-host VM lab assets

host_ip = ${host_ip}
target_host = ${target_host}
device = ${device}
device_bytes = ${device_bytes}
reserve_bytes = ${reserve_bytes}
target_count = ${target_count}
slice_bytes = ${slice_bytes}
kms_endpoints = ${kms_endpoints[*]}
kas_endpoints = ${kas_endpoints[*]}
quickstart_env = ${quickstart_env_path}
"
}

main() {
  local command=${1:-}
  [[ -n "${command}" ]] || {
    usage
    exit 1
  }
  shift || true

  case "${command}" in
    render-configs)
      render_configs_command "$@"
      ;;
    render-systemd)
      render_systemd_command "$@"
      ;;
    render-single-host-vm-lab)
      render_single_host_vm_lab_command "$@"
      ;;
    --help|-h|help)
      usage
      ;;
    *)
      die "unknown command: ${command}"
      ;;
  esac
}

main "$@"
