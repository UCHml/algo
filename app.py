# ===== Affinity to candidate genre: single-pass computation with 3 features =====
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

# Enable Adaptive Query Execution & skew handling (helps on large joins/shuffles)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Let Spark decide partitions adaptively (or set a concrete number if you prefer)
spark.conf.set("spark.sql.shuffle.partitions", "auto")


# ----------------------------- Helper: build pairs -----------------------------
def build_user_candidate_pairs(
    app_catalog_df: DataFrame,
    cross_usage_df: DataFrame,
    dim_apps_df: DataFrame,
    window_days: int = 3,
    company_apps_table: str = "prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps",
    device_agg: str = "avg"  # {"avg","max"}: how to aggregate cross-usage across device types
) -> DataFrame:
    """
    Returns 'pairs' with columns: user_id, cand_pid, aff
    where 'aff' are cross-usage edges A->B restricted to candidate's genre.
    """

    # Bridge mapping: package_name (slug) -> product_id (normalize to avoid silent misses)
    company_apps_map = (
        _get_company_apps_dim(company_apps_table)
        .select(
            F.lower(F.trim(F.col("product_slug"))).alias("product_slug"),
            F.col("product_id").cast("long").alias("product_id_long"),
        )
    )
    company_apps_map = F.broadcast(company_apps_map)  # small dim -> broadcast

    # Normalize base portfolio and filter time window / partner / system packages
    base = (
        app_catalog_df
        .select(
            F.col("date"),
            F.col("hashed_aaid").alias("user_id"),
            F.lower(F.trim(F.col("package_name"))).cast("string").alias("app_pid"),
            F.lower(F.trim(F.col("app_catalog_schedule"))).alias("sched"),
            F.col("is_system_package"),
            F.col("partner"),
        )
        .where(
            (F.col("is_system_package") == False)
            & (F.col("partner") == F.lit("motorola"))
            & (F.col("app_pid").isNotNull())
            & (F.col("date").between(F.date_sub(F.current_date(), window_days), F.current_date()))
        )
    )

    # Map to product_id
    base = (
        base.alias("app")
        .join(company_apps_map.alias("dim"), F.col("app.app_pid") == F.col("dim.product_slug"), "left")
        .select(
            F.col("app.date").alias("date"),
            F.col("app.user_id").alias("user_id"),
            F.col("app.app_pid").alias("app_pid"),
            F.col("app.sched").alias("sched"),
            F.col("dim.product_id_long").alias("product_id"),
        )
    )

    # Keep only 24hr / heart_beat snapshots
    base_f = base.where(F.col("sched").isin("24hr", "heart_beat")).select("date", "user_id", "app_pid", "product_id")

    # Anchor per user (latest date) and user portfolio on that date
    anchor_u = base_f.groupBy("user_id").agg(F.max("date").alias("d"))
    user_apps = (
        base_f.alias("b")
        .join(anchor_u.alias("a"), (F.col("b.user_id") == F.col("a.user_id")) & (F.col("b.date") == F.col("a.d")), "inner")
        .select(F.col("b.user_id"), F.col("b.product_id").alias("product_id"))
        .dropDuplicates(["user_id", "product_id"])
    )

    # PERFORMANCE: reduce cross-usage to only relevant 'from_pid' seen in current portfolios
    active_from = F.broadcast(user_apps.select(F.col("product_id").alias("from_pid")).distinct())

    # Cross-usage across ALL device types -> aggregate per (from_pid, to_pid)
    agg_expr = F.avg("aff") if device_agg == "avg" else F.max("aff")
    x = (
        cross_usage_df
        .select(
            F.col("product_id").cast("long").alias("from_pid"),
            F.col("cross_product_id").cast("long").alias("to_pid_long"),
            F.col("est_cross_product_affinity").alias("aff"),
        )
        .where(F.col("aff").isNotNull())
        .join(active_from, on="from_pid", how="inner")                # shrink early
        .groupBy("from_pid", "to_pid_long")
        .agg(agg_expr.alias("aff"))
    )

    # Repartition heavy DFs by join keys to reduce shuffle skew
    user_apps = user_apps.repartition("product_id")
    x = x.repartition("from_pid")

    # Genre for target apps (broadcast small dim)
    to_pid_with_genre = F.broadcast(
        dim_apps_df
        .select(
            F.col("product_id").cast("long").alias("to_pid_long"),
            F.lower(F.trim(F.col("product_unified_category_name"))).alias("to_genre"),
        )
    )

    # Candidate apps per user
    cand_raw = (
        user_apps.alias("ua")
        .join(x.alias("x"), F.col("x.from_pid") == F.col("ua.product_id"), "inner")
        .select(F.col("ua.user_id"), F.col("x.to_pid_long").alias("cand_pid_long"))
        .dropDuplicates(["user_id", "cand_pid_long"])
    )

    # Attach candidate genre (broadcast)
    dim_cand_genre = F.broadcast(
        dim_apps_df
        .select(
            F.col("product_id").cast("long").alias("cand_pid_long"),
            F.lower(F.trim(F.col("product_unified_category_name"))).alias("cand_genre"),
        )
    )
    cand_with_genre = (
        cand_raw.alias("cr")
        .join(dim_cand_genre.alias("d"), on="cand_pid_long", how="left")
        .select("cr.user_id", "cr.cand_pid_long", "d.cand_genre")
    )

    # Collect edges A->B that are inside candidate's genre
    pairs = (
        cand_with_genre.alias("cg")
        .join(user_apps.alias("ua"), F.col("ua.user_id") == F.col("cg.user_id"), "inner")
        .join(x.alias("x"), F.col("x.from_pid") == F.col("ua.product_id"), "inner")
        .join(to_pid_with_genre.alias("tg"), F.col("tg.to_pid_long") == F.col("x.to_pid_long"), "inner")
        .where(F.col("tg.to_genre") == F.col("cg.cand_genre"))
        .select(F.col("cg.user_id"), F.col("cg.cand_pid_long").alias("cand_pid"), F.col("x.aff"))
    )

    # Optionally persist if multiple downstream consumers will read it
    # pairs = pairs.persist()
    return pairs


# ------------------------- Load sources (example views) -------------------------
# Replace with your actual tables if names differ
app_catalog_df = spark.table("prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw")
cross_usage_df = spark.table("prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage")
dim_apps_df    = spark.table("prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps")


# --------------------- Build pairs ONCE and compute features -------------------
# Build once (you can tweak window_days or device_agg if needed)
pairs = build_user_candidate_pairs(
    app_catalog_df,
    cross_usage_df,
    dim_apps_df,
    window_days=3,     # default 3-day window
    device_agg="avg"   # or "max" to match another convention
).persist()            # materialize to avoid re-computation

# Single wide aggregation: one pass over data, 3 features at once
agg = (
    pairs.groupBy("user_id", "cand_pid")
    .agg(
        F.max("aff").cast("double").alias("f_affinity_max_genre"),
        F.avg("aff").cast("double").alias("f_affinity_avg_genre"),
        F.percentile_approx("aff", 0.75, 1000).cast("double").alias("f_affinity_p75_genre"),
    )
).persist()

# Slice into three feature DataFrames WITHOUT extra shuffles
feat_max = agg.select("user_id", "cand_pid", "f_affinity_max_genre")
feat_avg = agg.select("user_id", "cand_pid", "f_affinity_avg_genre")
feat_p75 = agg.select("user_id", "cand_pid", "f_affinity_p75_genre")

# Lightweight peek (avoid heavy count(); limit is cheap)
feat_max.limit(5).show(truncate=False)
feat_avg.limit(5).show(truncate=False)
feat_p75.limit(5).show(truncate=False)

# When done, free memory
pairs.unpersist()
agg.unpersist()

# Save as tables
# feat_max.write.mode("overwrite").saveAsTable("schema.f_affinity_max_genre")
# feat_avg.write.mode("overwrite").saveAsTable("schema.f_affinity_avg_genre")
# feat_p75.write.mode("overwrite").saveAsTable("schema.f_affinity_p75_genre")
