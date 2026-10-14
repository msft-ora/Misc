#!/usr/bin/env bash
# hdfs_small_file_scan.sh
#
# Usage:
#   ./hdfs_small_file_scan.sh <warehouse_path> [threshold_MB] [out_dir] [max_depth] [min_small_per_partition]
#
#   warehouse_path : REQUIRED, provided at runtime. e.g. /warehouse/db  /user/hive/warehouse
#   threshold_MB   : per-file threshold, default 128
#   out_dir        : default ./
#   max_depth      : how many path SEGMENTS below warehouse_path form the partition key.
#                    Table roll-up always uses the first 2 segments (<db>/<table>).
#                      2 -> <db>/<table>                (table-level, DEFAULT)
#                      3 -> <db>/<table>/dt=20261013    (partition-level)
#                      4 -> <db>/<table>/region/xxx     (sub-partition level)
#   min_small      : a partition counts as a SMALL-FILE PROBLEM only if it holds at least
#                    this many files below the threshold. Default 2, which means a lone
#                    small file is NOT counted:
#                      - partition with exactly 1 file, and that file < threshold  -> ignored
#                      - partition with many files but only 1 of them < threshold -> ignored
#                    Use 1 to disable this rule and count every sub-threshold file.
#
# Outputs (prefix small_files_<thr>MB_<timestamp>_):
#   files.tsv        REPORTABLE small files only (problem partitions):
#                    size_MB \t mtime \t db \t table \t partition_key \t repl \t full_path
#   files_all.tsv    EVERY sub-threshold file with a verdict column, sorted by verdict then size
#   partitions.tsv   partition_key, files, small_files, verdict, small_MB, total_MB, small_pct, last_modified
#   tables.tsv       table_key, bad_partitions, small_files, small_MB, total_MB, small_pct, last_modified
#   summary.txt      human readable roll-up
set -uo pipefail

WAREHOUSE="${1:?usage: $0 <warehouse_path> [threshold_MB] [out_dir] [max_depth] [min_small_per_partition]}"
THRESH_MB="${2:-128}"
OUTDIR="${3:-.}"
DEPTH="${4:-2}"
MIN_SMALL="${5:-2}"

case "$DEPTH"     in 1|2|3|4) ;; *) DEPTH=2 ;; esac
case "$MIN_SMALL" in 0|1|2|3|4|5) ;; *) MIN_SMALL=2 ;; esac

THRESH_BYTES=$(( THRESH_MB * 1024 * 1024 ))
STAMP=$(date +%Y%m%d_%H%M%S)
PREFIX="$OUTDIR/small_files_${THRESH_MB}MB_${STAMP}"
F_FILES="$PREFIX-files.tsv"
F_ALL="$PREFIX-files_all.tsv"
F_PARTS="$PREFIX-partitions.tsv"
F_TABLES="$PREFIX-tables.tsv"
F_SUM="$PREFIX-summary.txt"
F_RAW="$PREFIX-raw.tmp"
F_CNT="$PREFIX-cnt.tmp"

mkdir -p "$OUTDIR" || { echo "[err] cannot create out_dir: $OUTDIR" >&2; exit 1; }
TMP=$(mktemp "${TMPDIR:-/tmp}/hdfs_ls.XXXXXX") || exit 1
trap 'rm -f "$TMP" "$F_RAW" "$F_CNT"' EXIT

echo "[info] warehouse      : $WAREHOUSE" >&2
echo "[info] threshold      : ${THRESH_MB} MB per file" >&2
echo "[info] partition depth: $DEPTH segment(s) below warehouse" >&2
echo "[info] min small/part : $MIN_SMALL  (partitions below this are NOT reported)" >&2

# ---------- 1. recursive listing, regular files only ----------
hdfs dfs -ls -R "$WAREHOUSE" 2>/dev/null | grep -E '^-' > "$TMP"
TOTAL_FILES=$(wc -l < "$TMP" | tr -d ' ')
if [ "$TOTAL_FILES" -eq 0 ]; then
  echo "[err] no files found under $WAREHOUSE -- check the path" >&2
  exit 2
fi
echo "[info] files listed: $TOTAL_FILES" >&2

# ---------- 2. single awk pass: stats + staged raw rows + verdicts ----------
# hdfs dfs -ls field layout:
#   $1 perms  $2 replication  $3 owner  $4 group  $5 size(bytes, replicated)
#   $6 date  $7 time  $8.. path
#
# A partition's verdict can only be decided once ALL its files are seen, so sub-threshold
# rows are staged in F_RAW and classified in END, then bash splits them by verdict.
awk -v thr="$THRESH_BYTES" -v wh="$WAREHOUSE" -v depth="$DEPTH" -v minsmall="$MIN_SMALL" \
    -v F_PARTS="$F_PARTS" -v F_TABLES="$F_TABLES" -v F_RAW="$F_RAW" -v F_CNT="$F_CNT" \
    -v TOTAL="$TOTAL_FILES" '
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
  if (n == 0 || rel == path) {          # file sits directly in the warehouse root
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

  p2t[part]   = tblkey
  pcount[part]++
  pbytes[part] += fsize
  if (!(part in pmt)) pmt[part] = mtime
  tcount[tblkey]++
  tbytes[tblkey] += fsize
  if (!(tblkey in tmt)) tmt[tblkey] = mtime

  if (fsize < thr) {
    scount[part]++
    sbytes[part] += fsize
    raw_small++
    printf "%s\t%.2f\t%s\t%s\t%s\t%d\t%s\n", part, fsize / 1048576, mtime, db, tbl, repl, path > F_RAW
  }
  if (all % 200000 == 0)
    printf "[info]   ...%d/%d classified\n", all, TOTAL > "/dev/stderr"
}

END {
  for (k in pcount) {
    ns = scount[k] + 0
    if (ns >= minsmall) {
      verdict[k] = "SMALL_FILE_PROBLEM"
      badparts++
      bad_small    += ns
      bad_small_mb += sbytes[k]
      tk = p2t[k]
      tbadparts[tk]++
      tbadfiles[tk] += ns
      tbadbytes[tk] += sbytes[k]
    } else if (ns == 1) {
      verdict[k] = "isolated_single_small_file"
      iso_parts++
      iso_files++
      iso_bytes += sbytes[k]
    } else {
      verdict[k] = "ok_large_files_only"
    }
    ok_parts++
  }

  printf "partition_key\tfiles\tsmall_files\tverdict\tsmall_MB\ttotal_MB\tsmall_pct\tlast_modified\n" > F_PARTS
  for (k in pcount)
    printf "%s\t%d\t%d\t%s\t%.2f\t%.2f\t%.2f\t%s\n", k, pcount[k], scount[k] + 0, verdict[k],
           sbytes[k] / 1048576, pbytes[k] / 1048576,
           (pbytes[k] > 0 ? 100 * sbytes[k] / pbytes[k] : 0), pmt[k] > F_PARTS

  printf "table_key\tbad_partitions\tsmall_files\tsmall_MB\ttotal_MB\tsmall_pct\tlast_modified\n" > F_TABLES
  for (k in tcount)
    printf "%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%s\n", k, tbadparts[k] + 0, tbadfiles[k] + 0,
           tbadbytes[k] / 1048576, tbytes[k] / 1048576,
           (tbytes[k] > 0 ? 100 * tbadbytes[k] / tbytes[k] : 0), tmt[k] > F_TABLES

  printf "%d\t%d\t%d\t%.2f\t%d\t%d\t%.2f\t%d\t%.2f\n", \
         ok_parts + 0, badparts + 0, bad_small + 0, bad_small_mb / 1048576, \
         iso_parts + 0, iso_files + 0, iso_bytes / 1048576, \
         raw_small + 0, allbytes / 1048576 > F_CNT
}
' "$TMP"

# ---------- 3. attach verdicts, then split into reportable vs full audit ----------
awk -F'\t' '
  FNR == NR { if (FNR > 1) verdict[$1] = $4; next }
  {
    v = ($1 in verdict) ? verdict[$1] : "ok"
    printf "%s\t%s\t%.2f\t%s\t%s\t%s\t%d\t%s\n", v, $1, $2, $3, $4, $5, $6, $7
  }
' "$F_PARTS" "$F_RAW" | sort -t$'\t' -k1,1 -k3,3n > "$F_ALL"

awk -F'\t' '$1 == "SMALL_FILE_PROBLEM" { print $3 "\t" $4 "\t" $5 "\t" $6 "\t" $2 "\t" $7 "\t" $8 }' \
  "$F_ALL" | sort -t$'\t' -k1,1n > "$F_FILES"

# ---------- 4. summary ----------
SMALL_N=$(wc -l < "$F_FILES" | tr -d ' ')
{
  read -r OK_PARTS BAD_PARTS BAD_FILES BAD_MB ISO_PARTS ISO_FILES ISO_MB RAW_SMALL TOTAL_MB < "$F_CNT"
  echo "=== HDFS small-file scan ==="
  echo "timestamp        : $(date '+%F %T')"
  echo "warehouse        : $WAREHOUSE"
  echo "threshold        : < ${THRESH_MB} MB per file"
  echo "partition depth  : $DEPTH"
  echo "min small/part   : $MIN_SMALL"
  echo "files scanned    : $TOTAL_FILES  (${TOTAL_MB} MB)"
  echo "partitions seen  : $OK_PARTS"
  echo
  echo ">>> SMALL-FILE PROBLEMS (partitions with >= $MIN_SMALL files under ${THRESH_MB}MB)"
  echo "problem partitions: $BAD_PARTS"
  echo "small files       : $BAD_FILES  (${BAD_MB} MB)"
  echo
  echo "--- excluded ---"
  echo "partitions with exactly 1 small file : $ISO_PARTS  (${ISO_FILES} files, ${ISO_MB} MB)"
  echo "sub-threshold files overall          : $RAW_SMALL"
  echo
  echo "--- top 20 tables by problem small-file count ---"
  tail -n +2 "$F_TABLES" | sort -t$'\t' -k3 -nr | head -20 | \
    awk -F'\t' 'BEGIN { printf "%-42s %6s %8s %10s %10s %8s\n", "table", "bad_p", "small_n", "small_MB", "total_MB", "pct" }
                 { printf "%-42s %6s %8s %10s %10s %7s%%\n", $1, $2, $3, $4, $5, $6 }'
  echo
  echo "reportable files : $F_FILES"
  echo "full audit       : $F_ALL"
  echo "partitions       : $F_PARTS"
  echo "tables           : $F_TABLES"
} > "$F_SUM"

cat "$F_SUM" >&2
echo "[done] report prefix: $PREFIX" >&2