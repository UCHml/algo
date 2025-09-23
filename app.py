from pyspark.sql import functions as F

# Core builder: compute (user_id, cand_pid) aggregates once from input DataFrames
def affinity_agg_to_candidate_genre(app_catalog_df, cross_usage_df, dim_apps_df, window_days=30):
    # 1) filter portfolio (last `window_days`, only valid rows)
    base = (
        app_catalog_df
        .select(
            F.col("date"),
            F.col("hashed_aaid").alias("user_id"),
            F.col("package_name").cast("string").alias("app_pid"),
            F.lower(F.col("app_catalog_schedule")).alias("sched")
        )
        .where(
            (F.col("is_system_package") == False) &
            (F.col("partner") == F.lit("motorola")) &
            F.col("package_name").isNotNull() &
            (F.col("date").between(F.date_sub(F.current_date(), window_days), F.current_date()))
        )
    )

    # keep only 24hr / Heart_Beat
    base_f = base.where(F.col("sched").isin("24hr", "heart_beat")).select("date","user_id","app_pid")

    # per-user last available date (latest snapshot)
    anchor_u = base_f.groupBy("user_id").agg(F.max("date").alias("d"))

    # current portfolio: apps exactly on user's last day
    user_apps = (
        base_f.alias("b")
        .join(anchor_u.alias("a"), (F.col("b.user_id")==F.col("a.user_id")) & (F.col("b.date")==F.col("a.d")), "inner")
        .select(F.col("b.user_id"), F.col("b.app_pid").alias("product_id"))
        .dropDuplicates(["user_id","product_id"])
    )

    # cross-app affinity matrix (A -> B), device_code = 'all'
    x = (
        cross_usage_df
        .select(
            F.col("product_id").cast("string").alias("from_pid"),
            F.col("cross_product_id").cast("string").alias("to_pid"),
            F.col("est_cross_product_affinity").alias("aff"),
            F.col("device_code")
        )
        .where( (F.col("device_code")=="all") & F.col("aff").isNotNull() )
        .select("from_pid","to_pid","aff")
    )

    # to-app (B) genre
    to_pid_with_genre = (
        dim_apps_df
        .select(
            F.col("product_id").cast("string").alias("to_pid"),
            F.lower(F.trim(F.col("product_unified_category_name"))).alias("to_genre")
        )
    )

    # derive candidates from user's portfolio via cross-usage
    cand_raw = (
        user_apps.alias("ua")
        .join(x.alias("x"), F.col("x.from_pid")==F.col("ua.product_id"), "inner")
        .select(F.col("ua.user_id"), F.col("x.to_pid").alias("cand_pid"))
        .dropDuplicates(["user_id","cand_pid"])
    )

    # attach candidate genre
    cand_with_genre = (
        cand_raw.alias("cr")
        .join(
            dim_apps_df.select(
                F.col("product_id").cast("string").alias("cand_pid"),
                F.lower(F.trim(F.col("product_unified_category_name"))).alias("cand_genre")
            ).alias("d"),
            on="cand_pid",
            how="left"
        )
        .select("cr.user_id","cr.cand_pid","d.cand_genre")
    )

    # for each user app A, collect affinity to ALL apps B within candidate genre
    pairs = (
        cand_with_genre.alias("cg")
        .join(user_apps.alias("ua"), F.col("ua.user_id")==F.col("cg.user_id"), "inner")
        .join(x.alias("x"), F.col("x.from_pid")==F.col("ua.product_id"), "inner")
        .join(to_pid_with_genre.alias("tg"), F.col("tg.to_pid")==F.col("x.to_pid"), "inner")
        .where( F.col("tg.to_genre") == F.col("cg.cand_genre") )
        .select(F.col("cg.user_id"), F.col("cg.cand_pid"), F.col("x.aff"))
    )

    # final aggregates per (user, candidate_app)
    agg = (
        pairs.groupBy("user_id","cand_pid")
             .agg(
                 F.max("aff").alias("max_affinity_to_candidate_genre"),
                 F.avg("aff").alias("avg_affinity_to_candidate_genre"),
                 F.percentile_approx("aff", 0.75, 1000).alias("p75_affinity_to_candidate_genre")
             )
    )

    return agg


# Convenience selectors (no recomputation): split the single agg into 3 DFs
def max_affinity_to_candidate_genre_from_agg(agg_df):
    return agg_df.select("user_id","cand_pid","max_affinity_to_candidate_genre")

def avg_affinity_to_candidate_genre_from_agg(agg_df):
    return agg_df.select("user_id","cand_pid","avg_affinity_to_candidate_genre")

def p75_affinity_to_candidate_genre_from_agg(agg_df):
    return agg_df.select("user_id","cand_pid","p75_affinity_to_candidate_genre")
