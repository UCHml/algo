from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enable Adaptive Query Execution & skew handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Let Spark pick partitions adaptively (or set a sane number, e.g. 600–1200 if cluster big)
spark.conf.set("spark.sql.shuffle.partitions", "auto")

def build_user_candidate_pairs(
    app_catalog_df: DataFrame,
    cross_usage_df: DataFrame,
    dim_apps_df: DataFrame,
    window_days: int = 3,
    company_apps_table: str = "prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps",
    device_agg: str = "avg"  # {"avg","max"}
) -> DataFrame:
    # Bridge mapping: package_name (slug) -> product_id
    company_apps_map = (
        _get_company_apps_dim(company_apps_table)
        .select(
            F.lower(F.trim(F.col("product_slug"))).alias("product_slug"),
            F.col("product_id").cast("long").alias("product_id_long"),
        )
    )
    # Broadcast the small bridge
    company_apps_map = F.broadcast(company_apps_map)  # <<<<

    # Normalize base portfolio and filter
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

    base_f = base.where(F.col("sched").isin("24hr", "heart_beat")).select("date", "user_id", "app_pid", "product_id")

    # Anchor per user and portfolio for that day
    anchor_u = base_f.groupBy("user_id").agg(F.max("date").alias("d"))

    user_apps = (
        base_f.alias("b")
        .join(anchor_u.alias("a"), (F.col("b.user_id") == F.col("a.user_id")) & (F.col("b.date") == F.col("a.d")), "inner")
        .select(F.col("b.user_id"), F.col("b.product_id").alias("product_id"))
        .dropDuplicates(["user_id", "product_id"])
    )

    # ----- PERFORMANCE: reduce X early to only relevant from_pid -----
    # Distinct set of from_pid present in current users' portfolios
    active_from = user_apps.select(F.col("product_id").alias("from_pid")).distinct()
    # Broadcast if small enough
    active_from = F.broadcast(active_from)  # <<<< (safe if distinct products count is limited)

    # Build cross-usage matrix aggregated across all device types
    agg_expr = F.avg("aff") if device_agg == "avg" else F.max("aff")
    x_full = (
        cross_usage_df
        .select(
            F.col("product_id").cast("long").alias("from_pid"),
            F.col("cross_product_id").cast("long").alias("to_pid_long"),
            F.col("est_cross_product_affinity").alias("aff"),
        )
        .where(F.col("aff").isNotNull())
    )

    # Keep only edges where from_pid is in active user portfolio
    x = (
        x_full.join(active_from, on="from_pid", how="inner")  # <<<< dramatically shrinks X
            .groupBy("from_pid", "to_pid_long")
            .agg(agg_expr.alias("aff"))
    )

    # Repartition heavy DF by join keys to reduce shuffle skew
    user_apps = user_apps.repartition("product_id")      # <<<<
    x = x.repartition("from_pid")                        # <<<<

    # Genre for target apps (broadcast small dim)
    to_pid_with_genre = (
        dim_apps_df
        .select(
            F.col("product_id").cast("long").alias("to_pid_long"),
            F.lower(F.trim(F.col("product_unified_category_name"))).alias("to_genre"),
        )
    )
    to_pid_with_genre = F.broadcast(to_pid_with_genre)   # <<<<

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

    # Collect edges A->B within candidate's genre
    pairs = (
        cand_with_genre.alias("cg")
        .join(user_apps.alias("ua"), F.col("ua.user_id") == F.col("cg.user_id"), "inner")
        .join(x.alias("x"), F.col("x.from_pid") == F.col("ua.product_id"), "inner")
        .join(to_pid_with_genre.alias("tg"), F.col("tg.to_pid_long") == F.col("x.to_pid_long"), "inner")
        .where(F.col("tg.to_genre") == F.col("cg.cand_genre"))
        .select(F.col("cg.user_id"), F.col("cg.cand_pid_long").alias("cand_pid"), F.col("x.aff"))
    )

    # Optional: persist pairs if you compute several features afterwards
    # pairs = pairs.persist()

    return pairs



# Feature 1: max affinity
def f_affinity_max(pairs):
    return pairs.groupBy("user_id", "cand_pid").agg(
        F.max("aff").cast("double").alias("f_affinity_max_genre")
    )

# Feature 2: avg affinity
def f_affinity_avg(pairs):
    return pairs.groupBy("user_id", "cand_pid").agg(
        F.avg("aff").cast("double").alias("f_affinity_avg_genre")
    )

# Feature 3: 75th percentile affinity
def f_affinity_p75(pairs):
    return pairs.groupBy("user_id", "cand_pid").agg(
        F.percentile_approx("aff", 0.75, 1000).cast("double").alias("f_affinity_p75_genre")
    )


# --------- Example run in one cell ---------
# Build base pairs once
pairs = build_user_candidate_pairs(app_catalog_df, cross_usage_df, dim_apps_df)

# Compute features separately
feat_max = f_affinity_max(pairs)
feat_avg = f_affinity_avg(pairs)
feat_p75 = f_affinity_p75(pairs)

# Show counts / sample
print("pairs:", pairs.count())
print("feat_max:", feat_max.count())
print("feat_avg:", feat_avg.count())
print("feat_p75:", feat_p75.count())

feat_max.show(5, False)
feat_avg.show(5, False)
feat_p75.show(5, False)
