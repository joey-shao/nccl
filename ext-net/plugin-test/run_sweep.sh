#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ROLE=""
PLUGIN_TYPE="dpdk"
PLUGIN_PATH=""
NET_CONF="${SCRIPT_DIR}/net.conf"
DEV="0"
TASK_COUNT="4"
TIMEOUT_MS="1000000"
MIN_BYTES="1024"
MAX_BYTES="1073741824"
FACTOR="4"
OUT_FILE=""
VERBOSE="1"

usage() {
  cat <<'EOF'
Usage:
  ./run_sweep.sh --role sender|receiver --plugin-type dpdk|socket [options]

Options:
  --role <sender|receiver>       Test side to run.
  --plugin-type <dpdk|socket>    Select default plugin path.
  --plugin <path>                Override plugin .so path.
  --net-conf <path>              net.conf path. Default: ./net.conf
  --dev <idx>                    Device index. Default: 0
  --task-count <count>           Tasks per size. Default: 1
  --timeout-ms <ms>              Timeout per size. Default: 1000000
  --min-bytes <bytes>            First size. Default: 1024
  --max-bytes <bytes>            Last size. Default: 1073741824
  --factor <n>                   Size multiplier. Default: 2
  --out <path>                   Output log file.
  --verbose / --no-verbose       Pass --verbose to the test binary. Default: on
  -h, --help                     Show this help.

Run receiver first, then sender, with matching size/task parameters.
For DPDK, run the script with sudo -E if your environment requires it.
EOF
}

die() {
  echo "[ERR] $*" >&2
  exit 1
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --role)
      [[ $# -ge 2 ]] || die "--role needs a value"
      ROLE="$2"
      shift 2
      ;;
    --plugin-type|--type)
      [[ $# -ge 2 ]] || die "--plugin-type needs a value"
      PLUGIN_TYPE="$2"
      shift 2
      ;;
    --plugin)
      [[ $# -ge 2 ]] || die "--plugin needs a value"
      PLUGIN_PATH="$2"
      shift 2
      ;;
    --net-conf)
      [[ $# -ge 2 ]] || die "--net-conf needs a value"
      NET_CONF="$2"
      shift 2
      ;;
    --dev)
      [[ $# -ge 2 ]] || die "--dev needs a value"
      DEV="$2"
      shift 2
      ;;
    --task-count)
      [[ $# -ge 2 ]] || die "--task-count needs a value"
      TASK_COUNT="$2"
      shift 2
      ;;
    --timeout-ms)
      [[ $# -ge 2 ]] || die "--timeout-ms needs a value"
      TIMEOUT_MS="$2"
      shift 2
      ;;
    --min-bytes|--start-bytes)
      [[ $# -ge 2 ]] || die "--min-bytes needs a value"
      MIN_BYTES="$2"
      shift 2
      ;;
    --max-bytes|--end-bytes)
      [[ $# -ge 2 ]] || die "--max-bytes needs a value"
      MAX_BYTES="$2"
      shift 2
      ;;
    --factor)
      [[ $# -ge 2 ]] || die "--factor needs a value"
      FACTOR="$2"
      shift 2
      ;;
    --out)
      [[ $# -ge 2 ]] || die "--out needs a value"
      OUT_FILE="$2"
      shift 2
      ;;
    --verbose)
      VERBOSE="1"
      shift
      ;;
    --no-verbose)
      VERBOSE="0"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$ROLE" in
  sender|receiver) ;;
  "") die "--role is required" ;;
  *) die "--role must be sender or receiver" ;;
esac

case "$PLUGIN_TYPE" in
  dpdk|socket) ;;
  *) die "--plugin-type must be dpdk or socket" ;;
esac

for value in "$DEV" "$TASK_COUNT" "$TIMEOUT_MS" "$MIN_BYTES" "$MAX_BYTES" "$FACTOR"; do
  is_uint "$value" || die "numeric option has invalid value: $value"
done
(( TASK_COUNT > 0 )) || die "--task-count must be > 0"
(( TIMEOUT_MS > 0 )) || die "--timeout-ms must be > 0"
(( MIN_BYTES > 0 )) || die "--min-bytes must be > 0"
(( MAX_BYTES >= MIN_BYTES )) || die "--max-bytes must be >= --min-bytes"
(( FACTOR > 1 )) || die "--factor must be > 1"

if [[ -z "$PLUGIN_PATH" ]]; then
  case "$PLUGIN_TYPE" in
    dpdk) PLUGIN_PATH="${SCRIPT_DIR}/../dpdk/build/libnccl-net-dpdk.so" ;;
    socket) PLUGIN_PATH="${SCRIPT_DIR}/../socket/build/libnccl-net-socket.so" ;;
  esac
fi

BIN="${SCRIPT_DIR}/build/nccl-net-plugin-${ROLE}"
[[ -x "$BIN" ]] || die "test binary not found or not executable: $BIN (run: make -C ${SCRIPT_DIR} build)"
[[ -f "$PLUGIN_PATH" ]] || die "plugin not found: $PLUGIN_PATH"
[[ -f "$NET_CONF" ]] || die "net.conf not found: $NET_CONF"

if [[ "$PLUGIN_TYPE" == "dpdk" && "$(id -u)" != "0" ]]; then
  echo "[WARN] DPDK usually requires root. Re-run with sudo -E if EAL fails." >&2
fi

if [[ -z "$OUT_FILE" ]]; then
  mkdir -p "${SCRIPT_DIR}/results"
  OUT_FILE="${SCRIPT_DIR}/results/${ROLE}-${PLUGIN_TYPE}-$(date +%Y%m%d-%H%M%S).log"
else
  mkdir -p "$(dirname "$OUT_FILE")"
fi

SIZES=()
size="$MIN_BYTES"
while (( size <= MAX_BYTES )); do
  SIZES+=("$size")
  size=$(( size * FACTOR ))
done

{
  echo "# role=${ROLE}"
  echo "# plugin_type=${PLUGIN_TYPE}"
  echo "# plugin=${PLUGIN_PATH}"
  echo "# net_conf=${NET_CONF}"
  echo "# dev=${DEV}"
  echo "# task_count=${TASK_COUNT}"
  echo "# timeout_ms=${TIMEOUT_MS}"
  echo "# sizes_bytes=${SIZES[*]}"
  echo "# started_at=$(date '+%Y-%m-%d %H:%M:%S %z')"
} | tee -a "$OUT_FILE"

for task_bytes in "${SIZES[@]}"; do
  cmd=(
    "$BIN"
    --plugin "$PLUGIN_PATH"
    --net-conf "$NET_CONF"
    --dev "$DEV"
    --task-bytes "$task_bytes"
    --task-count "$TASK_COUNT"
    --timeout-ms "$TIMEOUT_MS"
  )
  if [[ "$VERBOSE" == "1" ]]; then
    cmd+=(--verbose)
  fi

  {
    echo
    echo "===== ${ROLE} ${PLUGIN_TYPE} task_bytes=${task_bytes} task_count=${TASK_COUNT} ====="
    printf '[CMD]'
    printf ' %q' "${cmd[@]}"
    echo
  } | tee -a "$OUT_FILE"

  set +e
  "${cmd[@]}" 2>&1 | tee -a "$OUT_FILE"
  status=${PIPESTATUS[0]}
  set -e

  if (( status != 0 )); then
    echo "[ERR] ${ROLE} failed for task_bytes=${task_bytes}, status=${status}" | tee -a "$OUT_FILE"
    exit "$status"
  fi
done

echo "# finished_at=$(date '+%Y-%m-%d %H:%M:%S %z')" | tee -a "$OUT_FILE"
echo "[DONE] wrote ${OUT_FILE}"
