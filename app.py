# avg app popularity per user (per-user last-day snapshot)
def user_avg_app_popularity(spark, window_days=30):
    return spark.sql(f"""
    WITH base AS (
        SELECT date,
               hashed_aaid AS user_id,
               package_name AS app_id
        FROM prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw
        WHERE date BETWEEN date_sub(current_date(), {window_days}) AND current_date()
          AND app_catalog_schedule IN ('24hr','Heart_Beat')
          AND is_system_package = false
          AND partner = 'motorola'
          AND package_name IS NOT NULL
    ),
    anchor_u AS (                 -- FIX: per-user MAX(date)
        SELECT user_id, MAX(date) AS d
        FROM base
        GROUP BY user_id
    ),
    now AS (                      -- строго на свой последний день
        SELECT DISTINCT b.user_id, b.app_id
        FROM base b
        JOIN anchor_u a
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


# avg app quality score per user (per-user last-day snapshot)
def user_avg_app_quality_score(spark, window_days=30):
    return spark.sql(f"""
    WITH base AS (
        SELECT date,
               hashed_aaid AS user_id,
               package_name AS app_id
        FROM prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw
        WHERE date BETWEEN date_sub(current_date(), {window_days}) AND current_date()
          AND app_catalog_schedule IN ('24hr','Heart_Beat')
          AND is_system_package = false
          AND partner = 'motorola'
          AND package_name IS NOT NULL
    ),
    anchor_u AS (                 -- FIX: per-user MAX(date)
        SELECT user_id, MAX(date) AS d
        FROM base
        GROUP BY user_id
    ),
    now AS (                      -- строго на свой последний день
        SELECT DISTINCT b.user_id, b.app_id
        FROM base b
        JOIN anchor_u a
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


# --- run & combine
df_popularity = user_avg_app_popularity(spark)
df_quality    = user_avg_app_quality_score(spark)

df_features = (
    df_popularity.alias("pop")
    .join(df_quality.alias("qual"), on="user_id", how="inner")
)

df_features.show(20, truncate=False)
