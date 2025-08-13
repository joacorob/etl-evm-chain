#!/usr/bin/env bash

set -euo pipefail

# Generates config.yaml using provided parameters or falling back to config.yaml.example defaults.
# Usage:
#   ./generate-config.sh \
#     --name "USDC" \
#     --address "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" \
#     --abi "./abi/token.json" \
#     --events "Transfer,Approval" \
#     [--output "/path/to/config.yaml"]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"
EXAMPLE_FILE="$ROOT_DIR/config.yaml.example"
OUTPUT_FILE="$ROOT_DIR/config.yaml"

print_usage() {
  cat <<EOF
Generate config.yaml from parameters or defaults in config.yaml.example

Flags:
  --name STRING        Contract name
  --address STRING     Contract address
  --abi STRING         Path to ABI JSON file
  --events STRING      Comma-separated list of event names (e.g. "Transfer,Approval")
  --output PATH        Output file path (default: $OUTPUT_FILE)
  -h, --help           Show this help message and exit

If a flag is omitted, its value is read from $EXAMPLE_FILE.
EOF
}

# Parse CLI args
contract_name=""
contract_address=""
contract_abi=""
events_csv=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      contract_name="${2:-}"; shift 2 ;;
    --address)
      contract_address="${2:-}"; shift 2 ;;
    --abi)
      contract_abi="${2:-}"; shift 2 ;;
    --events)
      events_csv="${2:-}"; shift 2 ;;
    --output)
      OUTPUT_FILE="${2:-}"; shift 2 ;;
    -h|--help)
      print_usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage
      exit 1 ;;
  esac
done

if [[ ! -f "$EXAMPLE_FILE" ]]; then
  echo "Example config not found: $EXAMPLE_FILE" >&2
  exit 1
fi

# Utility: extract top-level scalar value by key
extract_top_scalar() {
  local key="$1"
  awk -v k="$key" '
    BEGIN{FS=":"}
    $1 ~ "^"k"$" {
      sub(/^[^:]*:[[:space:]]*/, "", $0)
      sub(/[[:space:]]+#.*$/, "", $0)
      gsub(/^"|"$/, "", $0)
      print $0
      exit
    }
  ' "$EXAMPLE_FILE"
}

# Utility: extract nested scalar under a section (2-space indentation)
extract_nested_scalar() {
  local section="$1"; shift
  local key="$1"
  awk -v sec="$section" -v k="$key" '
    $0 ~ "^"sec":"$" { in=1; next }
    in==1 && $0 ~ /^[^[:space:]]/ { in=0 }
    in==1 && $0 ~ "^[[:space:]]{2}"k":" {
      line=$0
      sub(/^[[:space:]]*[^:]*:[[:space:]]*/, "", line)
      sub(/[[:space:]]+#.*$/, "", line)
      gsub(/^"|"$/, "", line)
      print line
      exit
    }
  ' "$EXAMPLE_FILE"
}

# Utility: extract nested scalar under section -> subsection (e.g., storage -> mysql -> dsn)
extract_double_nested_scalar() {
  local section="$1"; shift
  local subsection="$1"; shift
  local key="$1"
  awk -v sec="$section" -v subsec="$subsection" -v k="$key" '
    $0 ~ "^"sec":"$" { in1=1; next }
    in1==1 && $0 ~ "^[[:space:]]{2}"subsec":"$" { in2=1; next }
    in2==1 && $0 ~ "^[[:space:]]{2}[a-zA-Z0-9_]+:" { in2=0 }
    in1==1 && $0 ~ /^[^[:space:]]/ { in1=0 }
    in2==1 && $0 ~ "^[[:space:]]{4}"k":" {
      line=$0
      sub(/^[[:space:]]*[^:]*:[[:space:]]*/, "", line)
      sub(/[[:space:]]+#.*$/, "", line)
      gsub(/^"|"$/, "", line)
      print line
      exit
    }
  ' "$EXAMPLE_FILE"
}

# Extract defaults
rpc_url_default="$(extract_top_scalar "rpc_url")"
start_block_default="$(extract_top_scalar "start_block")"
chunk_size_default="$(extract_top_scalar "chunk_size")"

storage_type_default="$(extract_nested_scalar "storage" "type")"
mysql_dsn_default="$(extract_double_nested_scalar "storage" "mysql" "dsn")"
csv_outdir_default="$(extract_double_nested_scalar "storage" "csv" "output_dir")"

retry_attempts_default="$(extract_nested_scalar "retry" "attempts")"
retry_delay_ms_default="$(extract_nested_scalar "retry" "delay_ms")"

# Extract first contract defaults
readarray -t contract_defaults < <(awk '
  function unquote(s){ gsub(/^"|"$/, "", s); return s }
  $1=="contracts:" { inContracts=1; next }
  inContracts==1 && $1 ~ /^storage:/ { inContracts=0 }
  inContracts==1 {
    if ($1=="-" && $2 ~ /^name:/) {
      line=$0; sub(/^.*name:[[:space:]]*/, "", line); print "NAME:" unquote(line)
    }
    if ($1 ~ /^address:/) {
      line=$0; sub(/^.*address:[[:space:]]*/, "", line); print "ADDR:" unquote(line)
    }
    if ($1 ~ /^abi:/) {
      line=$0; sub(/^.*abi:[[:space:]]*/, "", line); print "ABI:" unquote(line)
    }
    if ($1 ~ /^events:/) { inEvents=1; next }
    if (inEvents==1 && $1=="-" ) {
      line=$0; sub(/^.*-[[:space:]]*/, "", line); print "EVENT:" unquote(line)
    }
    if (inEvents==1 && $1!="-" && $1!~/:/) { inEvents=0 }
  }
' "$EXAMPLE_FILE")

contract_name_default=""
contract_address_default=""
contract_abi_default=""

declare -a contract_events_default
for line in "${contract_defaults[@]}"; do
  case "$line" in
    NAME:*) contract_name_default="${line#NAME:}" ;;
    ADDR:*) contract_address_default="${line#ADDR:}" ;;
    ABI:*) contract_abi_default="${line#ABI:}" ;;
    EVENT:*) contract_events_default+=("${line#EVENT:}") ;;
  esac
done

# Resolve final values (provided or default)
final_name="${contract_name:-$contract_name_default}"
final_address="${contract_address:-$contract_address_default}"
final_abi="${contract_abi:-$contract_abi_default}"

declare -a final_events
if [[ -n "$events_csv" ]]; then
  IFS=',' read -r -a final_events <<< "$events_csv"
else
  final_events=("${contract_events_default[@]}")
fi

if [[ -z "$final_name" || -z "$final_address" || -z "$final_abi" ]]; then
  echo "Missing required contract defaults in $EXAMPLE_FILE and not provided via flags." >&2
  echo "Ensure the example has a contracts entry or pass --name, --address, and --abi." >&2
  exit 1
fi

# Generate YAML
{
  echo "rpc_url: \"$rpc_url_default\""
  echo "start_block: $start_block_default"
  echo "chunk_size: $chunk_size_default"
  echo ""
  echo "contracts:"
  echo "  - name: \"$final_name\""
  echo "    address: \"$final_address\""
  echo "    abi: \"$final_abi\""
  echo "    events:"
  if [[ ${#final_events[@]} -eq 0 ]]; then
    echo "      - \"\""
  else
    for ev in "${final_events[@]}"; do
      trimmed_ev=$(printf "%s" "$ev" | sed -e 's/^ *//' -e 's/ *$//')
      echo "      - \"$trimmed_ev\""
    done
  fi
  echo ""
  echo "storage:"
  echo "  type: \"$storage_type_default\""
  echo "  mysql:"
  echo "    dsn: \"$mysql_dsn_default\""
  echo "  csv:"
  echo "    output_dir: \"$csv_outdir_default\""
  echo ""
  echo "retry:"
  echo "  attempts: $retry_attempts_default"
  echo "  delay_ms: $retry_delay_ms_default"
} > "$OUTPUT_FILE"

echo "Wrote $OUTPUT_FILE"
