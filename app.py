from pyspark.sql import functions as F
from datetime import datetime, timedelta

def feat_user_inferred_affinity_flags_long(
    app_catalog_table="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    mapping_source_table="prod_atlas_datalake.unified_dimensions.app_taxonomy_mapping",
    run_date=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),  # logical date = yesterday
    z_threshold=1.0
):
    # snapshot of app catalog (same helper you already use)
    app_catalog_df = _get_app_catalog_single_snapshot(
        app_catalog_table=app_catalog_table,
        date=run_date
    )
    # app → genre mapping (same helper)
    mapping_df = _load_and_normalize_mapping(mapping_source_table=mapping_source_table)

    # user–app pairs (dedup)
    user_apps = (
        app_catalog_df
        .select("hashed_aaid", "package_name")
        .where(F.col("hashed_aaid").isNotNull() & F.col("package_name").isNotNull())
        .dropDuplicates(["hashed_aaid", "package_name"])
    )

    # join with genres (left; unknown → "unknown")
    joined = (
        user_apps.join(mapping_df.select("app_id","genre"),
                       on=user_apps["package_name"] == mapping_df["app_id"],
                       how="left")
                 .select(user_apps["hashed_aaid"].alias("user_id"),
                         user_apps["package_name"].alias("app_id"),
                         F.coalesce(F.col("genre"), F.lit("unknown")).alias("genre"))
    )

    # per-user counts by genre
    genre_counts = (
        joined.groupBy("user_id","genre")
              .agg(F.countDistinct("app_id").alias("n_apps"))
    )
    # per-user total mapped apps
    totals = (
        genre_counts.groupBy("user_id")
                    .agg(F.sum("n_apps").alias("user_total_mapped_apps"))
    )
    # proportions (user_id, genre, proportion)
    props = (
        genre_counts.join(totals, "user_id")
                    .withColumn("proportion", F.col("n_apps")/F.col("user_total_mapped_apps"))
                    .select("user_id","genre","proportion")
    )

    # global genre stats across users
    stats = (
        props.groupBy("genre")
             .agg(F.avg("proportion").alias("mu"),
                  F.stddev_pop("proportion").alias("sigma"))
    )

    # z-score per (user, genre)
    ztab = (
        props.join(stats, "genre")
             .withColumn(
                 "z",
                 F.when((F.col("sigma").isNull()) | (F.col("sigma") == 0), F.lit(0.0))
                  .otherwise( (F.col("proportion") - F.col("mu"))/F.col("sigma") )
             )
    )

    # binary flag + feature_name
    flags = (
        ztab.select(
            F.lit(run_date).cast("date").alias("date"),
            F.col("user_id").alias("hashed_aaid"),
            F.concat(
                F.lit("user_inferred_affinity_"),
                F.regexp_replace(F.lower(F.col("genre")), "[^a-z0-9]+", "_")
            ).alias("feature_name"),
            F.lit("bigint").alias("feature_type"),
            F.when(F.col("z") > F.lit(z_threshold), F.lit(1)).otherwise(F.lit(0))
             .cast("string").alias("feature_value")
        )
    )

    return flags
