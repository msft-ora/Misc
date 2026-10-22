import re
from pyspark.sql import DataFrame, Window
from pyspark.sql.column import Column
from pyspark.sql.functions import coalesce, col, count, expr, last, lit, max, sum, when
_R_AGGS = ("sum", "count")                                  # supported aggregates
_OTHER_AGGS = ("avg", "mean", "min", "max", "first", "last", "stddev",
               "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp",
               "countdistinct", "approx_count_distinct", "collect_list",
               "collect_set", "median", "percentile", "skewness", "kurtosis")
_AGG_CALL = re.compile(r"(?<![\w.])(%s)\s*\(" % "|".join(_R_AGGS + _OTHER_AGGS),
                       re.IGNORECASE)
def _norm_keys(key, df_columns):
    cols = (key,) if isinstance(key, str) else tuple(key)
    if not cols or not all(isinstance(c, str) and c for c in cols):
        raise ValueError(f"partition_col must be a column name or a non-empty "
                         f"list of column names, got: {key!r}")
    for c in cols:
        if c.startswith("__"):
            raise ValueError(f"partition_col '{c}' uses the reserved '__' prefix")
        if c not in df_columns:
            raise ValueError(f"partition_col '{c}' not in df columns")
    return cols
def _match_paren(s, i):
    """Index of the ')' matching the '(' at s[i], honouring quotes; -1 if none."""
    depth, quote = 0, None
    for j in range(i, len(s)):
        ch = s[j]
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return j
    return -1
def _strip_outer(s):
    """Drop parens that wrap the WHOLE string: '(SUM(A))' -> 'SUM(A)'."""
    s = s.strip()
    while len(s) > 1 and s[0] == "(" and _match_paren(s, 0) == len(s) - 1:
        s = s[1:-1].strip()
    return s
def _top_level_call(body):
    """'sum(a * b)' -> ('sum', 'a * b'); anything that is not exactly one
    call covering the whole string -> (None, body). Only the NAME and the raw
    inner text are returned; the inner text is never re-parsed here."""
    body = _strip_outer(body)
    i, n = 0, len(body)
    while i < n and (body[i].isalnum() or body[i] == "_"):
        i += 1
    if 0 < i < n and body[i] == "(":
        j = _match_paren(body, i)
        if j == n - 1:
            return body[:i].lower(), body[i + 1:j]
    return None, body
def _plain_name(s):
    """'X' / 'a.b' / '`weird name`' -> the bare column name, else None."""
    s = s.strip()
    if re.fullmatch(r"`[^`]+`", s):
        return s[1:-1]
    if re.fullmatch(r"[A-Za-z_]\w*(\.[A-Za-z_]\w*)*", s):
        return s
    return None
def _measure_of(fc):
    """col_exp -> (agg, mkey, measure_col). agg is "sum"|"count"; measure_col
    is the aggregate Column used directly in the groupBy. Accepted forms:
      SQL string:  "SUM(BAD_PIN_ATTMPT_IND)", "COUNT(TRANS_ID)",
                   "SUM(col1 * col2)", "SUM(CASE WHEN ... END)", "COUNT(*)"
                   (function name in any case, full-width parens normalised)
      Column:      sum(col("X")), count("ID"), sum(col("A") * col("B")),
                   count(when(...))
      bare form:   "X" or "col1 * col2" or col("A") * col("B") -> sum
    Only a single plain SUM/COUNT is supported (no DISTINCT, no arithmetic
    around the aggregate). Plain-column-ref inners normalise to (fn, name)
    so SUM(X), "X" and sum(col("X")) all share ONE measure."""
    if isinstance(fc, Column):
        m = re.match(r"^Column<'(.*)'>$", str(fc), re.DOTALL)
        body, wrapped = (m.group(1) if m else str(fc)), fc
    elif isinstance(fc, str):
        body = fc.replace("（", "(").replace("）", ")").strip()
        if not body:
            raise ValueError("col_exp is an empty string")
        name = _plain_name(body)
        if name is not None:                        # bare column name -> sum
            return "sum", ("sum", name), sum(col(name))
        wrapped = None
    else:
        raise TypeError(f"col_exp must be a SQL string (\"SUM(X)\", "
                        f"\"COUNT(ID)\", ...) or a Column expression "
                        f"(sum(col('X')), count('ID'), ...), got "
                        f"{type(fc).__name__}")
    fn, inner = _top_level_call(body)
    if fn is None:                                  # bare expression -> sum
        if _AGG_CALL.search(body):
            raise ValueError(f"col_exp must be ONE SUM(...) or COUNT(...) call "
                             f"covering the whole expression - no extra "
                             f"arithmetic/aggregates around it: '{body}'")
        if wrapped is not None:
            return "sum", ("sum_obj", str(fc)), sum(wrapped)
        return "sum", ("sum_sql", " ".join(body.split())), sum(expr(body))
    if fn not in _R_AGGS:
        raise ValueError(f"only SUM(...)/COUNT(...) are supported in col_exp, "
                         f"got {fn.upper()}(...) - no other aggregates")
    if inner.strip().upper().startswith("DISTINCT"):
        raise ValueError("COUNT(DISTINCT ...)/SUM(DISTINCT ...) are not "
                         "supported in col_exp")
    name = _plain_name(inner)
    if name is not None:
        return fn, (fn, name), (sum if fn == "sum" else count)(col(name))
    if wrapped is not None:
        return fn, (fn + "_obj", str(fc)), wrapped
    return fn, (fn + "_sql", " ".join(inner.split())), expr(body)
def _spec_lookups(base, mcols, sid_specs, measures):
    """Passes 1-3 (+ the null-ts peer-group value) for ONE partition-key group.
    ONE aggregation over the raw rows feeds everything: the per-second series
    (cumulative anchors), the lookup timestamps, and the null-ts group value.
    sid_specs: [(sid, normalized spec)]; measures: mkey -> (agg, agg Column)."""
    sids = [sid for sid, _ in sid_specs]
    spec_of = dict(sid_specs)
    mkeys = sorted(measures, key=str)
    midx = {k: i for i, k in enumerate(mkeys)}
    not_null = lit(True)
    for c in mcols:
        not_null = not_null & col(c).isNotNull()
    # Pass 1: raw rows -> one row per (key..., second), evaluating each spec's
    # OWN aggregate Column per second (sum -> null when all values that second
    # are null; count -> non-null count, any column type, no casts). The
    # null-__ts group is this same aggregation applied to all null-ts rows of
    # the key - exactly the peer-group frame the original window uses.
    per_sec = (base.where(not_null).groupBy(*mcols, "__ts")
                   .agg(*[measures[k][1].alias(f"__m_{i}")
                          for i, k in enumerate(mkeys)]))
    real = per_sec.where(col("__ts").isNotNull())
    # Pass 2: per key, cumulative value per measure over the tiny second
    # series; sum measures also get a cumulative COUNT of non-null per-second
    # sums (a frame with zero of them => Spark sum() must return null, not 0).
    # Anchors are REAL seconds only: the null-ts group is reported separately,
    # so the prefix sums stay pure and can never leak into a difference.
    w_cum = (Window.partitionBy(*mcols).orderBy("__ts")
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    cum_cols = []
    for i, k in enumerate(mkeys):
        cum_cols.append(sum(f"__m_{i}").over(w_cum).alias(f"__cv_{i}"))
        if measures[k][0] == "sum":
            cum_cols.append(count(f"__m_{i}").over(w_cum).alias(f"__cz_{i}"))
    anchors = (real.select(*mcols, col("__ts").alias("__x"), *cum_cols)
                   .withColumn("__orig_ts", lit(None).cast("long"))
                   .withColumn("__spec", lit(None).cast("string"))
                   .withColumn("__which", lit(None).cast("string"))
                   .withColumn("__kind", lit(0)))            # 0 = anchor
    cum_types = {f.name: f.dataType for f in anchors.schema.fields}
    cum_names = [f.name for f in anchors.schema.fields
                 if f.name.startswith(("__cv_", "__cz_"))]
    # Pass 3: asof lookups at both frame edges for every spec.
    # rangeBetween(-w, -1) inclusive => [t-w, t-1] = cum(t-1) - cum(t-w-1).
    # 'real' is already one row per (key, second), so no distinct is needed
    # here - the request frames are plain projections of the pass-1 output.
    reqs = []
    for sid in sids:
        w = spec_of[sid]["window_seconds"]
        for edge, off in (("hi", 1), ("lo", w + 1)):
            reqs.append(real.select(*mcols,
                                    (col("__ts") - off).alias("__x"),
                                    col("__ts").alias("__orig_ts"))
                            .withColumn("__spec", lit(sid))
                            .withColumn("__which", lit(edge)))
    requests = reqs[0]
    for r in reqs[1:]:
        requests = requests.unionByName(r)
    requests = (requests.select(*mcols, "__x", "__orig_ts", "__spec", "__which",
                                *[lit(None).cast(cum_types[c]).alias(c)
                                  for c in cum_names])
                        .withColumn("__kind", lit(1)))       # 1 = request
    # Union anchors + requests; an anchor sorts before a request at the same
    # __x, so the fill-forward is an inclusive asof join.
    w_ff = (Window.partitionBy(*mcols).orderBy("__x", "__kind")
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    merged = anchors.unionByName(requests)
    for c in cum_names:  # same WindowSpec -> Spark runs them in ONE WindowExec
        merged = merged.withColumn(c, last(c, ignorenulls=True).over(w_ff))
    filled = merged.where(col("__kind") == 1).drop("__kind", "__x")
    v = coalesce(*[when(col("__spec") == sid,
                        col(f"__cv_{midx[spec_of[sid]['mkey']]}"))
                   for sid in sids])
    sel = [*mcols, "__orig_ts", "__spec", "__which", v.alias("__vv")]
    sum_sids = [sid for sid in sids if spec_of[sid]["agg"] == "sum"]
    if sum_sids:
        z = coalesce(*[when(col("__spec") == sid,
                            col(f"__cz_{midx[spec_of[sid]['mkey']]}"))
                       for sid in sum_sids])
        sel.append(coalesce(z, lit(0)).alias("__vz"))
    else:
        sel.append(lit(0).alias("__vz"))
    filled = filled.select(*sel)
    # Per (key..., ts, spec): frame value = hi - lo.
    #   sum:   null when the frame holds no non-null value
    #          (Spark: empty frame / all-null frame -> sum() = null)
    #   count: non-null values in frame (Spark count: empty frame -> 0)
    per_spec = (filled.groupBy(*mcols, "__orig_ts", "__spec")
                      .agg(max(when(col("__which") == "hi", col("__vv"))).alias("hv"),
                           max(when(col("__which") == "lo", col("__vv"))).alias("lv"),
                           max(when(col("__which") == "hi", col("__vz"))).alias("hz"),
                           max(when(col("__which") == "lo", col("__vz"))).alias("lz")))
    out = []
    for sid in sids:
        s = spec_of[sid]
        if s["agg"] == "count":
            out.append(max(when(col("__spec") == sid,
                                coalesce(col("hv"), lit(0))
                                - coalesce(col("lv"), lit(0))))
                         .alias(s["feature_name"]))
        else:
            out.append(max(when(col("__spec") == sid,
                                when(col("hz") - col("lz") == 0, lit(None))
                                .otherwise(col("hv") - coalesce(col("lv"), lit(0)))))
                         .alias(s["feature_name"]))
    lookup = (per_spec.groupBy(*mcols, "__orig_ts").agg(*out)
                      .withColumnRenamed("__orig_ts", "__ts"))
    # Spark quirk: rows with NULL ts form a peer group inside the partition
    # whose range frame = the whole null-ts group (INCLUDING the current row),
    # so the original window returns the aggregate over all null-ts rows of
    # that key. Reading it off the same per_sec aggregation reproduces it
    # exactly (sum -> null if every value in the group is null, count -> 0).
    null_out = (per_sec.where(col("__ts").isNull())
                       .select(*mcols,
                               *[col(f"__m_{midx[spec_of[sid]['mkey']]}")
                                 .alias(f"__n_{sid}") for sid in sids]))
    return lookup, null_out
def add_merchant_sliding_counts(
    df: DataFrame,
    specs: list,
    ts_col: str = "TRANS_DT",
) -> DataFrame:
    """
    Skew-safe replacement for sliding rangeBetween windows. Each spec's col_exp
    carries its own aggregation, so a spec maps 1:1 onto the original code:
        sum(col("BAD_PIN_ATTMPT_IND")).over(w)   -> col_exp = "BAD_PIN_ATTMPT_IND"
                                                     or sum(col("BAD_PIN_ATTMPT_IND"))
                                                     or "SUM(BAD_PIN_ATTMPT_IND)"
        count(col("TRANS_ID")).over(w)           -> col_exp = "COUNT(TRANS_ID)"
        sum(col("A") * col("B")).over(w)         -> col_exp = "SUM(A * B)"
        count(when(col("ST") == "FAIL", 1)).over(w)
                                                 -> col_exp = "COUNT(CASE WHEN ST = 'FAIL' THEN 1 END)"
    where w = Window.partitionBy(<spec partition_col>)
                    .orderBy(col(<ts_col>).cast('timestamp').cast('long'))
                    .rangeBetween(-window_seconds, -1)
    and the whole original expression is wrapped in when(<key not null>, ...).
    Each spec: {"feature_name": ..., "partition_col": ..., "col_exp": ...,
                "window_seconds": ...}   (there is no separate 'agg' key)
      - col_exp: the aggregation over the frame, in ANY of these forms
          SQL string:   "SUM(BAD_PIN_ATTMPT_IND)", "COUNT(TRANS_ID)",
                        "SUM(col1 * col2)", "COUNT(*)",
                        "SUM(CASE WHEN AMT > 100 THEN AMT ELSE 0 END)"
                        (function name in any case; full-width parens OK)
          Column:       sum(col("X")), count("ID"), sum(col("A") * col("B")),
                        count(when(col("ST") == "FAIL", 1))
          bare form:    "X", "col1 * col2" or col("A") * col("B")
                        -> defaults to SUM
        Only ONE plain SUM/COUNT covering the whole expression is supported:
        no DISTINCT, no other aggregates, no arithmetic around the aggregate
        (e.g. "SUM(A) + 1" is rejected). SQL strings are evaluated with expr()
        exactly as written, so anything Spark SQL parses inside SUM(...)/
        COUNT(...) works.
      - sum: nulls skipped; null when the frame is empty or all values are
        null (exact Spark sum semantics). Result keeps Spark's natural sum
        type (int->bigint, decimal->decimal, double->double). NOTE: for
        float/double columns the summation ORDER differs from the original
        window (per-second groups vs row-by-row), so results can differ in
        the last ulp - Spark's own window is likewise order-dependent among
        same-second peer rows. Integer and decimal columns are exact.
      - count: number of NON-NULL values in the frame, counted on the
        ORIGINAL column/expression (any type - string ids are fine, nothing
        is cast). 0 when the frame is empty. COUNT(*) counts rows.
      - partition_col: THIS feature's partition key - a column name or a
        list, e.g. "DRVD_MRCH_ID" or ["DRVD_MRCH_ID", "CHANNEL_CD"]. Specs
        sharing the same key share one pipeline. Avoid non-deterministic
        col_exp expressions (rand(), current_timestamp(), ...).
      - Rows where ANY of the spec's key columns is null get null in that
        spec's feature_name only.
    Spark semantics are preserved EXACTLY, including the null-ts quirk:
    null-ts rows form a peer group whose frame is the whole group (sum ->
    group sum, count -> group non-null count).
    """
    if not specs:
        return df
    if ts_col not in df.columns:
        raise ValueError(f"ts_col '{ts_col}' not in df columns")
    # ---- validate + normalize specs (never mutates the caller's dicts) ----
    seen = set()
    sid_specs = []                                       # (sid, spec, key, mcol)
    for i, s in enumerate(specs):
        for key in ("feature_name", "partition_col", "col_exp", "window_seconds"):
            if key not in s:
                raise ValueError(f"spec missing required key '{key}': {s}")
        if "agg" in s:
            raise ValueError("the 'agg' key no longer exists - write the "
                             "aggregation into col_exp instead, e.g. "
                             "sum(col('X')), 'SUM(X)' or count('TRANS_ID')")
        fn = s["feature_name"]
        if not isinstance(fn, str) or not fn:
            raise ValueError(f"feature_name must be a non-empty str: {s}")
        if fn.startswith("__"):
            raise ValueError(f"feature_name '{fn}' uses the reserved '__' prefix")
        if fn in seen:
            raise ValueError(f"duplicate feature_name '{fn}'")
        w = s["window_seconds"]
        if isinstance(w, bool) or not isinstance(w, int) or w <= 0:
            raise ValueError(f"window_seconds must be a positive int: {s}")
        seen.add(fn)
        agg, mkey, mcol = _measure_of(s["col_exp"])
        if mkey[0] in _R_AGGS and mkey[1] not in df.columns:
            raise ValueError(f"col_exp column '{mkey[1]}' not in df columns")
        ns = {"feature_name": fn, "agg": agg, "mkey": mkey, "window_seconds": w}
        sid_specs.append((f"__s{i}", ns,
                          _norm_keys(s["partition_col"], df.columns), mcol))
    # ---- group specs by partition key; one shared pipeline per group ----
    groups = {}
    for sid, ns, key_tuple, mcol in sid_specs:
        groups.setdefault(key_tuple, []).append((sid, ns, mcol))
    clash = set(seen) & {c for kt in groups for c in kt}
    if clash or ts_col in seen:
        raise ValueError(f"feature_name(s) {sorted(clash | ({ts_col} & seen))} "
                         f"collide with a key/ts column")
    base = df.withColumn("__ts", col(ts_col).cast("timestamp").cast("long"))
    result = base
    helper_cols = []
    for key_tuple, group in groups.items():
        measures = {}
        for sid, ns, mcol in group:
            measures[ns["mkey"]] = (ns["agg"], mcol)
        lookup, null_out = _spec_lookups(
            base, key_tuple, [(sid, ns) for sid, ns, _ in group], measures)
        result = result.join(lookup, list(key_tuple) + ["__ts"], "left")
        result = result.join(null_out, list(key_tuple), "left")
        for sid, ns, _ in group:
            result = result.withColumn(
                ns["feature_name"],
                when(col("__ts").isNull(), col(f"__n_{sid}"))
                 .otherwise(col(ns["feature_name"])))
            helper_cols.append(f"__n_{sid}")
    return result.drop("__ts", *helper_cols)
