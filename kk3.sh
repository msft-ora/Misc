#!/bin/bash
# find_small_files.sh — scan a Hive DB path, auto-detect each table's partition
# level, flag files < threshold only when their partition has 2+ small files.
#
# Usage: find_small_files.sh <db_path> [threshold_mb]   (default 128)

set -euo pipefail

DB_PATH=${1:?Usage: $0 <hdfs_db_path> [threshold_mb]}
THRESH_MB=${2:-128}
LIMIT=$(( THRESH_MB * 1024 * 1024 ))
REPORT="small_files_report_$(date +%Y%m%d_%H%M%S).txt"
DETECT=$(mktemp)

echo "Scanning DB: $DB_PATH (threshold ${THRESH_MB} MB, partition depth auto-detected per table)"
echo "Scanning DB: $DB_PATH (threshold ${THRESH_MB} MB)" > "$REPORT"

hdfs dfs -ls -R "$DB_PATH" | \
awk -v db="$DB_PATH" -v limit="$LIMIT" '
  $1 ~ /^-/ {
    # rebuild full path (fields 8..NF; filenames may contain spaces)
    path = $8
    for (i = 9; i <= NF; i++) path = path " " $i

    n   = split(path, c, "/")
    ndb = split(db, dc, "/")
    reln = n - ndb
    if (reln < 2) next                        # file directly under DB path - skip

    table = c[ndb + 1]

    # skip hidden / staging components: .hive-staging, _temporary, _SUCCESS, ...
    skip = 0
    for (i = ndb + 1; i <= n; i++)
      if (c[i] ~ /^[._]/) { skip = 1; break }
    if (skip) next

    depth = reln - 1                          # dirs between table dir and file
    if (depth == 0) { nonpart[table] = 1; next }   # non-partitioned table

    key = table
    for (i = ndb + 1; i <= n; i++) key = key "\034" c[i]
    fsize[key] = $5
    fpath[key] = path
    fcount[table]++
    if (!(table in mindepth) || depth < mindepth[table]) mindepth[table] = depth
  }
  END {
    # group files by (table, partition path at that tables detected depth)
    for (key in fsize) {
      m = split(key, k, "\034")               # k[1]=table, k[2..m]=comps, k[m]=filename
      table = k[1]
      d = mindepth[table]
      depth = m - 2
      if (depth > d) { deeper[table]++; continue }  # nested deeper than partition level
      pkey = table
      for (i = 2; i <= d + 1; i++) pkey = pkey "\034" k[i]
      psize[pkey, pcount[pkey]++] = fsize[key]
      ppath[pkey, pcount[pkey] - 1] = fpath[key]
    }

    for (p in pcount) {
      small = 0
      for (i = 0; i < pcount[p]; i++)
        if (psize[p,i] < limit) small++
      if (small > 1) {                        # rule: 2+ small files in the partition
        for (i = 0; i < pcount[p]; i++)
          if (psize[p,i] < limit) {
            printf "%10.2f MB\t%s\n", psize[p,i]/1048576, ppath[p,i]
            files++
          }
        parts++
      }
    }

    # detection summary -> stderr (gets appended to the report too)
    printf "\n== Table scan summary ==\n" > "/dev/stderr"
    for (t in nonpart)
      printf "table=%s  non-partitioned (skipped)\n" t > "/dev/stderr"
    for (t in mindepth) {
      extra = (t in deeper) ? sprintf("  [WARN: %d file(s) nested deeper than partition level - ignored]", deeper[t]) : ""
      printf "table=%s  partition depth=%d  files=%d%s\n", t, mindepth[t], fcount[t], extra > "/dev/stderr"
    }
    printf "== Result ==\nPartitions flagged: %d | Small files flagged: %d\n", parts+0, files+0 > "/dev/stderr"
  }' 2> "$DETECT" | sort -k2 | tee -a "$REPORT"

echo "" >> "$REPORT"
cat "$DETECT" >> "$REPORT"
rm -f "$DETECT"

echo ""
echo "Report written to: $(pwd)/$REPORT"
