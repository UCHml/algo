from pyspark.sql import functions as F

# Helper to build user-candidate pairs with affinity edges
def build_user_candidate_pairs(
    app_catalog_df,
    cross_usage_df,
    dim_apps_df,
    window_days=3,
    company_apps_table="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps",
    device_agg="avg"  # {"avg","max"}: how to aggregate cross-usage affinity across device types
):
    # Bridge mapping: package_name (slug) -> product_id
    company_apps_map = (
        _get_company_apps_dim(company_apps_table)
        .select(
            F.lower(F.trim(F.col("product_slug"))).alias("product_slug"),
            F.col("product_id").cast("long").alias("product_id_long"),
        )
    )

    # Normalize base portfolio: clean package_name/schedule, filter window/system/partner
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
        .join(
            F.broadcast(company_apps_map).alias("dim"),
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

    # Keep only 24hr/heartbeat snapshots
    base_f = (
        base.where(F.col("sched").isin("24hr", "heart_beat"))
            .select("date", "user_id", "app_pid", "product_id")
    )

    # Anchor = latest date per user
    anchor_u = base_f.groupBy("user_id").agg(F.max("date").alias("d"))

    # User portfolio on the anchor date
    user_apps = (
        base_f.alias("b")
        .join(
            anchor_u.alias("a"),
            (F.col("b.user_id") == F.col("a.user_id")) & (F.col("b.date") == F.col("a.d")),
            "inner",
        )
        .select(F.col("b.user_id"), F.col("b.product_id").alias("product_id"))
        .dropDuplicates(["user_id", "product_id"])
    )

    # Cross-usage matrix aggregated across all device types
    agg_expr = F.avg("aff") if device_agg == "avg" else F.max("aff")

    x = (
        cross_usage_df
        .select(
            F.col("product_id").cast("long").alias("from_pid"),
            F.col("cross_product_id").cast("long").alias("to_pid_long"),
            F.col("est_cross_product_affinity").alias("aff"),
        )
        .where(F.col("aff").isNotNull())
        .groupBy("from_pid", "to_pid_long")
        .agg(agg_expr.alias("aff"))
    )

    # Genre for target apps
    to_pid_with_genre = (
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

    # Attach candidate genre
    cand_with_genre = (
        cand_raw.alias("cr")
        .join(
            dim_apps_df
            .select(
                F.col("product_id").cast("long").alias("cand_pid_long"),
                F.lower(F.trim(F.col("product_unified_category_name"))).alias("cand_genre"),
            ).alias("d"),
            on="cand_pid_long",
            how="left",
        )
        .select("cr.user_id", "cr.cand_pid_long", "d.cand_genre")
    )

    # Collect affinity edges only within candidate's genre
    pairs = (
        cand_with_genre.alias("cg")
        .join(user_apps.alias("ua"), F.col("ua.user_id") == F.col("cg.user_id"), "inner")
        .join(x.alias("x"), F.col("x.from_pid") == F.col("ua.product_id"), "inner")
        .join(to_pid_with_genre.alias("tg"), F.col("tg.to_pid_long") == F.col("x.to_pid_long"), "inner")
        .where(F.col("tg.to_genre") == F.col("cg.cand_genre"))
        .select(F.col("cg.user_id"), F.col("cg.cand_pid_long").alias("cand_pid"), F.col("x.aff"))
    )

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
