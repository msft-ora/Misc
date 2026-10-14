#!/usr/bin/env bash
# hdfs_small_file_scan.sh
#
# Usage:
#   ./hdfs_small_file_scan.sh <warehouse_path> [threshold_MB] [out_dir] [max_depth]
#
#   warehouse_path : REQUIRED, provided at runtime. e.g. /warehouse/db  /user/hive/warehouse
#   threshold_MB   : per-file threshold, default 128
#   out_dir        : default ./
#   max_depth      : how many path SEGMENTS below warehouse_path form the partition key.
#                    Table roll-up always uses the first 2 segments (<db>/<table>).
#                      2 -> <db>/<table>                (table-level, DEFAULT)
#                      3 -> <db>/<table>/dt=20261013    (partition-level)
#                      4 -> <db>/<table>/region/xxx     (sub-partition level)
#
# Outputs (all prefixed small_files_<thr>MB_<timestamp>_):
#   files.tsv       size_MB \t mtime \t db \t table \t partition_key \t replication \t full_path
#   partitions.tsv  partition_key, files, small_files, small_MB, total_MB, small_pct, last_modified
#   tables.tsv      table_key, files, small_files, small_MB, total_MB, small_pct, last_modified
#   summary.txt     human readable roll-up
set -uo pipefail

WAREHOUSE="${1:?usage: $0 <warehouse_path> [threshold_MB] [out_dir] [max_depth]}"
THRESH_MB="${2:-128}"
OUTDIR="${3:-.}"
DEPTH="${4:-2}"

case "$DEPTH" in 1|2|3|4) ;; *) DEPTH=2 ;; esac
THRESH_BYTES=$(( THRESH_MB * 1024 * 1024 ))
STAMP=$(date +%Y%m%d_%H%M%S)
PREFIX="$OUTDIR/small_files_${THRESH_MB}MB_${STAMP}"
F_FILES="$PREFIX-files.tsv"
F_PARTS="$PREFIX-partitions.tsv"
F_TABLES="$PREFIX-tables.tsv"
F_SUM="$PREFIX-summary.txt"

mkdir -p "$OUTDIR" || { echo "[err] cannot create out_dir: $OUTDIR" >&2; exit 1; }
TMP=$(mktemp "${TMPDIR:-/tmp}/hdfs_ls.XXXXXX") || exit 1
trap 'rm -f "$TMP"' EXIT

echo "[info] warehouse      : $WAREHOUSE" >&2
echo "[info] threshold      : ${THRESH_MB} MB per file" >&2
echo "[info] partition depth: $DEPTH segment(s) below warehouse" >&2

# ---------- 1. recursive listing, regular files only ----------
hdfs dfs -ls -R "$WAREHOUSE" 2>/dev/null | grep -E '^-' > "$TMP"
TOTAL_FILES=$(wc -l < "$TMP" | tr -d ' ')
if [ "$TOTAL_FILES" -eq 0 ]; then
  echo "[err] no files found under $WAREHOUSE -- check the path" >&2
  exit 2
fi
echo "[info] files listed: $TOTAL_FILES" >&2

# ---------- 2. classify, emit per-file rows + two roll-ups ----------
# hdfs dfs -ls field layout:
#   $1 perms  $2 replication  $3 owner  $4 group  $5 size(bytes, replicated)
#   $6 date  $7 time  $8.. path
awk -v thr="$THRESH_BYTES" -v wh="$WAREHOUSE" -v depth="$DEPTH" \
    -v F_PARTS="$F_PARTS" -v F_TABLES="$F_TABLES" -v TOTAL="$TOTAL_FILES" '
function logical(bytes, repl) { return (repl + 0 > 0 ? bytes / repl : bytes) }
function trim(s) { sub(/^\/+/, "", s); sub(/\/+$/, "", s); return s }

{
  repl  = $2 + 0
  size  = $5 + 0
  mtime = $6 " " $7
  $1 = $2 = $3 = $4 = $5 = $6 = $7 = ""
  sub(/^ +/, "")
  path = $0
  if (path == "") next

  fsize = logical(size, repl)
  all++
  allbytes += fsize

  # ---- derive db / table / partition key from path ----
  rel = trim(substr(path, length(wh) + 1))
  n = split(rel, seg, "/")
  if (n == 0 || rel == path) {          # file sits directly in warehouse root
    db = "-"; tbl = "-"; part = wh
  } else {
    db  = (n >= 1 ? seg[1] : "-")
    tbl = (n >= 2 ? seg[2] : "-")
    if (n >= 3 && depth >= 3) {
      if      (depth == 3) part = db "/" tbl "/" seg[3]
      else if (depth == 4) part = db "/" tbl "/" seg[3] "/" seg[4]
      else                 part = db "/" tbl "/" seg[3] "/" seg[4] "/" seg[5]
    } else {
      part = db "/" tbl
    }
  }
  tblkey = (db == "-" && tbl == "-") ? wh : db "/" tbl

  tcount[tblkey]++
  tbytes[tblkey] += fsize
  if (!(tblkey in tmt)) tmt[tblkey] = mtime

  pcount[part]++
  pbytes[part] += fsize
  if (!(part in pmt)) pmt[part] = mtime

  if (fsize < thr) {
    small++
    smallbytes += fsize
    tscount[tblkey]++
    tsbytes[tblkey] += fsize
    scount[part]++
    sbytes[part] += fsize
    printf "%.2f\t%s\t%s\t%s\t%s\t%d\t%s\n", fsize / 1048576, mtime, db, tbl, part, repl, path
  }
  if (all % 200000 == 0)
    printf "[info]   ...%d/%d classified\n", all, TOTAL > "/dev/stderr"
}

END {
  printf "partition_key\tfiles\tsmall_files\tsmall_MB\ttotal_MB\tsmall_pct\tlast_modified\n" > F_PARTS
  for (k in pcount)
    printf "%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%s\n", k, pcount[k], scount[k] + 0,
           sbytes[k] / 1048576, pbytes[k] / 1048576,
           (pbytes[k] > 0 ? 100 * sbytes[k] / pbytes[k] : 0), pmt[k] > F_PARTS

  printf "table_key\tfiles\tsmall_files\tsmall_MB\ttotal_MB\tsmall_pct\tlast_modified\n" > F_TABLES
  for (k in tcount)
    printf "%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%s\n", k, tcount[k], tscount[k] + 0,
           tsbytes[k] / 1048576, tbytes[k] / 1048576,
           (tbytes[k] > 0 ? 100 * tsbytes[k] / tbytes[k] : 0), tmt[k] > F_TABLES
}
' "$TMP" | sort -n > "$F_FILES"

# ---------- 3. summary ----------
SMALL_N=$(wc -l < "$F_FILES" | tr -d ' ')
SMALL_MB=$(awk -F'\t' '{ s += $1 } END { printf "%.2f", s + 0 }' "$F_FILES")
AFFECTED=$(awk -F'\t' 'NR > 1 && $3 > 0' "$F_TABLES" | wc -l | tr -d ' ')

{
  echo "=== HDFS small-file scan ==="
  echo "timestamp       : $(date '+%F %T')"
  echo "warehouse       : $WAREHOUSE"
  echo "threshold       : < ${THRESH_MB} MB per file"
  echo "partition depth : $DEPTH"
  echo "files scanned   : $TOTAL_FILES"
  echo "small files     : $SMALL_N  (${SMALL_MB} MB)"
  echo "affected tables : $AFFECTED"
  echo
  echo "--- top 20 tables by small-file count ---"
  tail -n +2 "$F_TABLES" | sort -t$'\t' -k3 -nr | head -20 | \
    awk -F'\t' 'BEGIN { printf "%-44s %8s %9s %10s %8s\n", "table", "small_n", "small_MB", "total_MB", "pct" }
                 { printf "%-44s %8s %9s %10s %7s%%\n", $1, $3, $4, $5, $6 }'
  echo
  echo "files       : $F_FILES"
  echo "partitions  : $F_PARTS"
  echo "tables      : $F_TABLES"
} > "$F_SUM"

cat "$F_SUM" >&2
echo "[done] report prefix: $PREFIX" >&2