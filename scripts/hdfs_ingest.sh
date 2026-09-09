#!/usr/bin/env bash
# hdfs_ingest.sh â€” Upload locally acquired CSVs to HDFS raw layer.
#
# Prerequisites:
#   - hadoop CLI in PATH
#   - HDFS_NAMENODE set in .env or exported in environment
#   - data/raw/ already populated by acquire.py
#
# Usage:
#   bash scripts/hdfs_ingest.sh
#   bash scripts/hdfs_ingest.sh --verify   (check block counts after upload)

set -euo pipefail

# â”€â”€ Load .env â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
fi

HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://localhost:9000}"
LOCAL_RAW="data/raw"
HDFS_RAW="${HDFS_NAMENODE}/user/agmarknet/raw"
HDFS_PROCESSED="${HDFS_NAMENODE}/user/agmarknet/processed"

VERIFY=false
if [[ "${1:-}" == "--verify" ]]; then
  VERIFY=true
fi

# â”€â”€ Sanity checks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if ! command -v hdfs &> /dev/null; then
  echo "ERROR: 'hdfs' command not found. Is Hadoop in your PATH?"
  echo "If running in local-only mode, set USE_HDFS=false in .env."
  exit 1
fi

if [ ! -d "$LOCAL_RAW" ]; then
  echo "ERROR: $LOCAL_RAW does not exist. Run acquire.py first."
  exit 1
fi

# â”€â”€ Create HDFS directories â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "[1/3] Creating HDFS directories..."
hdfs dfs -mkdir -p "${HDFS_RAW}"
hdfs dfs -mkdir -p "${HDFS_PROCESSED}"
echo "      ${HDFS_RAW}"
echo "      ${HDFS_PROCESSED}"

# â”€â”€ Upload raw CSVs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "[2/3] Uploading raw CSVs..."
LOCAL_COUNT=$(find "$LOCAL_RAW" -name "*.csv" | wc -l)
echo "      Found ${LOCAL_COUNT} CSV files in ${LOCAL_RAW}"

# Use -f to overwrite existing files; remove .done sentinels before upload
find "$LOCAL_RAW" -name ".done" -delete

hdfs dfs -put -f "${LOCAL_RAW}/"* "${HDFS_RAW}/"

echo "      Upload complete."

# â”€â”€ List and optionally verify â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "[3/3] Listing HDFS raw layer..."
hdfs dfs -ls -R "${HDFS_RAW}" | tail -20
HDFS_COUNT=$(hdfs dfs -ls -R "${HDFS_RAW}" | grep -c "\.csv" || true)
echo "      ${HDFS_COUNT} CSV files visible in HDFS"

if $VERIFY; then
  echo ""
  echo "--- Block count verification ---"
  hdfs dfs -count "${HDFS_RAW}"
  echo "Raw HDFS path used:"
  hdfs dfs -du -s -h "${HDFS_RAW}"
fi

echo ""
echo "Done. HDFS raw layer ready at: ${HDFS_RAW}"
echo ""
echo "Useful follow-up commands:"
echo "  hdfs dfs -ls -R ${HDFS_RAW} | head -30   # browse files"
echo "  hdfs dfs -du -h ${HDFS_RAW}               # total size"
echo "  python src/process.py --sample             # smoke-test processing"
echo "Next step: python src/process.py"

