from pyspark.sql import functions as F

# Internal helper: compute per-(user, candidate_app) affinity aggregates to candidate GENRE
def _affinity_agg_to_candidate_genre(
    candidates_table,  # columns: hashed_aaid, candidate_product_id
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    return spark.sql(f"""
    WITH base AS (
      -- user app presence with required filters (normalize schedule values)
      SELECT date,
             hashed_aaid            AS user_id,
             package_name           AS app_pid,
             LOWER(app_catalog_schedule) AS sched
      FROM {app_catalog}
      WHERE is_system_package = false
        AND partner = 'motorola'
        AND package_name IS NOT NULL
    ),
    base_f AS (
      -- keep only 24hr / Heart_Beat rows irrespective of case
      SELECT date, user_id, app_pid
      FROM base
      WHERE sched IN ('24hr','heart_beat')
    ),
    anchor AS (SELECT MAX(date) AS d FROM base_f),

    user_apps_30d AS (
      -- distinct user apps in the last 30 days
      SELECT DISTINCT user_id, CAST(app_pid AS STRING) AS product_id
      FROM base_f b CROSS JOIN anchor a
      WHERE b.date BETWEEN date_sub(a.d, 30) AND a.d
    ),

    cand AS (
      -- candidate apps per user
      SELECT hashed_aaid AS user_id,
             CAST(candidate_product_id AS STRING) AS cand_pid
      FROM {candidates_table}
    ),

    cand_with_genre AS (
      -- map candidate app -> its unified category (genre)
      SELECT c.user_id,
             c.cand_pid,
             LOWER(TRIM(d.product_unified_category_name)) AS cand_genre
      FROM cand c
      LEFT JOIN {dim_apps} d
        ON c.cand_pid = CAST(d.product_id AS STRING)
    ),

    x AS (
      -- cross-app affinity matrix (A -> B), only device_code='all'
      SELECT CAST(product_id AS STRING)       AS from_pid,
             CAST(cross_product_id AS STRING) AS to_pid,
             est_cross_product_affinity       AS aff
      FROM {cross_usage}
      WHERE device_code = 'all'
        AND est_cross_product_affinity IS NOT NULL
    ),

    to_pid_with_genre AS (
      -- map destination app (B) -> its unified category (genre)
      SELECT CAST(product_id AS STRING) AS to_pid,
             LOWER(TRIM(product_unified_category_name)) AS to_genre
      FROM {dim_apps}
    ),

    pairs AS (
      -- for each user app A, take affinity to all apps B within candidate genre
      SELECT cg.user_id,
             cg.cand_pid,
             ua.product_id AS user_app_pid,
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

def max_affinity_to_candidate_genre(
    candidates_table,
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    # Return max affinity only
    df = _affinity_agg_to_candidate_genre(candidates_table, app_catalog, cross_usage, dim_apps)
    return df.select(F.col("user_id"), F.col("cand_pid"), F.col("max_affinity_to_candidate_genre"))

def avg_affinity_to_candidate_genre(
    candidates_table,
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    # Return average affinity only
    df = _affinity_agg_to_candidate_genre(candidates_table, app_catalog, cross_usage, dim_apps)
    return df.select(F.col("user_id"), F.col("cand_pid"), F.col("avg_affinity_to_candidate_genre"))

def p75_affinity_to_candidate_genre(
    candidates_table,
    app_catalog="prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    cross_usage="prod_one_dt_datalake.lakehouse_reporting.dataai_cross_app_usage",
    dim_apps="prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps"
):
    # Return 75th percentile affinity only
    df = _affinity_agg_to_candidate_genre(candidates_table, app_catalog, cross_usage, dim_apps)
    return df.select(F.col("user_id"), F.col("cand_pid"), F.col("p75_affinity_to_candidate_genre"))
