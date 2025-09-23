from pyspark.sql import functions as F

# 1) Normalize keys in portfolio and in the bridge mapping (avoid silent join misses)
base = (
    app_catalog_df
    .select(
        F.col("date"),
        F.col("hashed_aaid").alias("user_id"),
        F.lower(F.trim(F.col("package_name"))).cast("string").alias("app_pid"),  # normalize package
        F.lower(F.trim(F.col("app_catalog_schedule"))).alias("sched"),
        F.col("is_system_package"),
        F.col("partner"),
    )
    .where(
        (F.col("is_system_package") == False) &
        (F.col("partner") == F.lit("motorola")) &
        (F.col("app_pid").isNotNull()) &
        (F.col("date").between(F.date_sub(F.current_date(), window_days), F.current_date()))
    )
)

company_apps_map = (
    _get_company_apps_dim(company_apps_table)
    .select(
        F.lower(F.trim(F.col("product_slug"))).alias("product_slug"),   # normalize slug
        F.col("product_id").cast("long").alias("product_id_long")       # use one numeric type
    )
)

# Map app_pid -> product_id (bridge join)
base = (
    base.alias("app")
    .join(
        F.broadcast(company_apps_map).alias("dim"),                     # small dim -> broadcast
        F.col("app.app_pid") == F.col("dim.product_slug"),
        "left",
    )
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

# Latest available date per user (anchor)
anchor_u = base_f.groupBy("user_id").agg(F.max("date").alias("d"))

# User portfolio on the latest day (distinct products)
user_apps = (
    base_f.alias("b")
    .join(
        anchor_u.alias("a"),
        (F.col("b.user_id") == F.col("a.user_id")) & (F.col("b.date") == F.col("a.d")),
        "inner"
    )
    .select(F.col("b.user_id"), F.col("b.product_id").alias("product_id"))
    .dropDuplicates(["user_id", "product_id"])
)

# 2) Build cross-usage matrix X; relax/normalize device_code to avoid empty result
x = (
    cross_usage_df
    .select(
        F.col("product_id").cast("long").alias("from_pid"),
        F.col("cross_product_id").cast("long").alias("to_pid_long"),
        F.col("est_cross_product_affinity").alias("aff"),
        F.lower(F.trim(F.col("device_code"))).alias("device_code_norm"),
    )
    # keep rows with non-null affinity and with 'all'-type device buckets
    .where(
        (F.col("aff").isNotNull()) &
        (
            (F.col("device_code_norm").isNull()) |
            (F.col("device_code_norm").isin("all", "all_devices"))
        )
    )
    .select("from_pid", "to_pid_long", "aff")
)

# Genre for target (to) apps
to_pid_with_genre = (
    dim_apps_df
    .select(
        F.col("product_id").cast("long").alias("to_pid_long"),
        F.lower(F.trim(F.col("product_unified_category_name"))).alias("to_genre"),
    )
)

# Derive candidate apps for each user through cross-usage
cand_raw = (
    user_apps.alias("ua")
    .join(x.alias("x"), F.col("x.from_pid") == F.col("ua.product_id"), "inner")
    .select(F.col("ua.user_id"), F.col("x.to_pid_long").alias("cand_pid_long"))
    .dropDuplicates(["user_id", "cand_pid_long"])
)

# Attach candidate genre
cand_with_genre = (
    cand_raw.alias("cr")
    .join(
        dim_apps_df
        .select(
            F.col("product_id").cast("long").alias("cand_pid_long"),
            F.lower(F.trim(F.col("product_unified_category_name"))).alias("cand_genre"),
        ).alias("d"),
        on="cand_pid_long", how="left"
    )
    .select("cr.user_id", "cr.cand_pid_long", "d.cand_genre")
)

# For each user app A, collect affinity to all apps B within candidate genre
pairs = (
    cand_with_genre.alias("cg")
    .join(user_apps.alias("ua"), F.col("ua.user_id") == F.col("cg.user_id"), "inner")
    .join(x.alias("x"), F.col("x.from_pid") == F.col("ua.product_id"), "inner")
    .join(to_pid_with_genre.alias("tg"), F.col("tg.to_pid_long") == F.col("x.to_pid_long"), "inner")
    .where(F.col("tg.to_genre") == F.col("cg.cand_genre"))
    .select(F.col("cg.user_id"), F.col("cg.cand_pid_long").alias("cand_pid"), F.col("x.aff"))
)

# Final aggregates per (user, candidate_app)
agg = (
    pairs.groupBy("user_id", "cand_pid")
    .agg(
        F.max("aff").alias("max_affinity_to_candidate_genre"),
        F.avg("aff").alias("avg_affinity_to_candidate_genre"),
        F.percentile_approx("aff", 0.75, 1000).alias("p75_affinity_to_candidate_genre"),
    )
)

# Quick sanity counters for debugging (remove or wrap with a debug flag in prod)
print("base_f        :", base_f.count())
print("anchor_u      :", anchor_u.count())
print("user_apps     :", user_apps.count())
print("x (cross-usage):", x.count())  # should be > 0

print(
    "distinct device_code in raw cross_usage:",
    cross_usage_df.select(F.lower(F.trim(F.col("device_code"))).alias("dc"))
                  .groupBy("dc").count().orderBy(F.desc("count")).limit(20).collect()
)

print("cand_raw      :", cand_raw.count())          # if 0 -> X is empty / type mismatch
print("cand_with_genre:", cand_with_genre.count())  # if 0 -> missing genre for candidate
print("pairs         :", pairs.count())             # if 0 -> genre join didn't match
