# Function: average app popularity per user
def user_avg_app_popularity(spark):
    return spark.sql("""
    WITH base AS (
        SELECT date,
               hashed_aaid AS user_id,
               package_name AS app_id
        FROM prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw
        WHERE date BETWEEN date_sub(current_date(), 30) AND current_date()
          AND app_catalog_schedule IN ('24hr','Heart_Beat')
          AND is_system_package = false
          AND partner = 'motorola'
          AND package_name IS NOT NULL
    ),
    anchor AS (
        SELECT user_id, MAX(date) AS d
        FROM base
        GROUP BY user_id
    ),
    now AS (
        SELECT DISTINCT b.user_id, b.app_id
        FROM base b
        JOIN anchor a
          ON b.user_id = a.user_id AND b.date = a.d
    ),
    pop AS (
        SELECT CAST(product_id AS STRING) AS app_id,
               COALESCE(est_install_base, est_average_active_users) AS popularity
        FROM prod_one_dt_datalake.lakehouse_reporting.dataai_usage
        WHERE device_code = 'all'
          AND (est_install_base IS NOT NULL OR est_average_active_users IS NOT NULL)
    )
    SELECT n.user_id,
           AVG(p.popularity) AS user_avg_app_popularity
    FROM now n
    JOIN pop p
      ON n.app_id = p.app_id
    GROUP BY n.user_id
    """)


# Function: average app quality score per user
def user_avg_app_quality_score(spark):
    return spark.sql("""
    WITH base AS (
        SELECT date,
               hashed_aaid AS user_id,
               package_name AS app_id
        FROM prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw
        WHERE date BETWEEN date_sub(current_date(), 30) AND current_date()
          AND app_catalog_schedule IN ('24hr','Heart_Beat')
          AND is_system_package = false
          AND partner = 'motorola'
          AND package_name IS NOT NULL
    ),
    anchor AS (
        SELECT user_id, MAX(date) AS d
        FROM base
        GROUP BY user_id
    ),
    now AS (
        SELECT DISTINCT b.user_id, b.app_id
        FROM base b
        JOIN anchor a
          ON b.user_id = a.user_id AND b.date = a.d
    ),
    churn AS (
        SELECT CAST(product_id AS STRING) AS app_id,
               app_global_churn_rate AS churn_rate
        FROM prod_one_dt_datalake.lakehouse_reporting.dataai_usage
        WHERE device_code = 'all'
          AND app_global_churn_rate IS NOT NULL
    )
    SELECT n.user_id,
           AVG(c.churn_rate) AS user_avg_app_quality_score
    FROM now n
    JOIN churn c
      ON n.app_id = c.app_id
    GROUP BY n.user_id
    """)
    

# --- Run both features and join results
df_popularity = user_avg_app_popularity(spark)
df_quality = user_avg_app_quality_score(spark)

df_features = (
    df_popularity.alias("pop")
    .join(df_quality.alias("qual"), on="user_id", how="inner")
)

df_features.show(20, truncate=False)




from pyspark.sql import functions as F

# Internal helper: build (user, candidate_app) from user portfolio and cross-app usage,
# then aggregate affinities by candidate GENRE over the last 30 days.
def _affinity_agg_to_candidate_genre_no_candidates(
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    return spark.sql(f"""
    WITH base AS (
      -- user portfolio with required filters
      SELECT
        date,
        hashed_aaid              AS user_id,
        CAST(package_name AS STRING) AS app_pid,
        LOWER(app_catalog_schedule) AS sched
      FROM {app_catalog}
      WHERE is_system_package = false
        AND partner = 'motorola'
        AND package_name IS NOT NULL
    ),
    base_f AS (
      -- keep only 24hr / Heart_Beat (case-insensitive)
      SELECT date, user_id, app_pid
      FROM base
      WHERE sched IN ('24hr','heart_beat')
    ),
    anchor AS (
      -- latest available date
      SELECT MAX(date) AS d FROM base_f
    ),
    user_apps_30d AS (
      -- distinct user apps in the last 30 days
      SELECT DISTINCT b.user_id, b.app_pid AS product_id
      FROM base_f b
      CROSS JOIN anchor a
      WHERE b.date BETWEEN date_sub(a.d, 30) AND a.d
    ),
    x AS (
      -- cross-app affinity matrix (A -> B), aggregated device level
      SELECT
        CAST(product_id       AS STRING) AS from_pid,
        CAST(cross_product_id AS STRING) AS to_pid,
        est_cross_product_affinity       AS aff
      FROM {cross_usage}
      WHERE device_code = 'all'
        AND est_cross_product_affinity IS NOT NULL
    ),
    to_pid_with_genre AS (
      -- destination app (B) -> unified category (genre)
      SELECT
        CAST(product_id AS STRING) AS to_pid,
        LOWER(TRIM(product_unified_category_name)) AS to_genre
      FROM {dim_apps}
    ),
    cand_raw AS (
      -- derive candidate apps from user's portfolio via cross-usage
      SELECT DISTINCT ua.user_id, x.to_pid AS cand_pid
      FROM user_apps_30d ua
      JOIN x ON x.from_pid = ua.product_id
    ),
    cand_with_genre AS (
      -- attach candidate genre
      SELECT
        cr.user_id,
        cr.cand_pid,
        LOWER(TRIM(d.product_unified_category_name)) AS cand_genre
      FROM cand_raw cr
      LEFT JOIN {dim_apps} d
        ON cr.cand_pid = CAST(d.product_id AS STRING)
    ),
    pairs AS (
      -- for each user app A, collect its affinity to ALL apps B within candidate genre
      SELECT
        cg.user_id,
        cg.cand_pid,
        x.aff
      FROM cand_with_genre cg
      JOIN user_apps_30d ua
        ON ua.user_id = cg.user_id
      JOIN x
        ON x.from_pid = ua.product_id
      JOIN to_pid_with_genre tg
        ON tg.to_pid = x.to_pid
       AND tg.to_genre = cg.cand_genre
    )
    SELECT
      user_id,
      cand_pid,
      MAX(aff)                           AS max_affinity_to_candidate_genre,
      AVG(aff)                           AS avg_affinity_to_candidate_genre,
      percentile_approx(aff, 0.75, 1000) AS p75_affinity_to_candidate_genre
    FROM pairs
    GROUP BY user_id, cand_pid
    """)

# Return max-affinity only: user_id, cand_pid, max_affinity_to_candidate_genre
def max_affinity_to_candidate_genre(
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    df = _affinity_agg_to_candidate_genre_no_candidates(app_catalog, cross_usage, dim_apps)
    return df.select(F.col("user_id"), F.col("cand_pid"), F.col("max_affinity_to_candidate_genre"))

# Return avg-affinity only: user_id, cand_pid, avg_affinity_to_candidate_genre
def avg_affinity_to_candidate_genre(
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    df = _affinity_agg_to_candidate_genre_no_candidates(app_catalog, cross_usage, dim_apps)
    return df.select(F.col("user_id"), F.col("cand_pid"), F.col("avg_affinity_to_candidate_genre"))

# Return p75-affinity only: user_id, cand_pid, p75_affinity_to_candidate_genre
def p75_affinity_to_candidate_genre(
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    df = _affinity_agg_to_candidate_genre_no_candidates(app_catalog, cross_usage, dim_apps)
    return df.select(F.col("user_id"), F.col("cand_pid"), F.col("p75_affinity_to_candidate_genre"))



# 1) compute each metric
df_max  = max_affinity_to_candidate_genre()
df_avg  = avg_affinity_to_candidate_genre()
df_p75  = p75_affinity_to_candidate_genre()

# 2) join into a single wide df: (user_id, cand_pid, max, avg, p75)
df_aff = (
    df_max
    .join(df_avg, on=["user_id", "cand_pid"], how="outer")
    .join(df_p75, on=["user_id", "cand_pid"], how="outer")
)
