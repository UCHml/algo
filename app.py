def feat_user_avg_app_popularity():
    return spark.sql("""
    WITH base AS (  -- фильтр «присутствия» приложений
      SELECT date, hashed_aaid AS user_id, package_name AS app_id
      FROM prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw
      WHERE app_catalog_schedule IN ('24hr','Heart_Beat')
        AND is_system_package = false
        AND partner = 'motorola'
        AND package_name IS NOT NULL
    ),
    anchor AS (     -- ЯКОРЬ ПО КАЖДОМУ ЮЗЕРУ
      SELECT user_id, MAX(date) AS d
      FROM base
      GROUP BY user_id
    ),
    now AS (        -- текущее окно вокруг личного якоря
      SELECT DISTINCT b.user_id, b.app_id
      FROM base b
      JOIN anchor a
        ON b.user_id = a.user_id
       AND b.date BETWEEN date_sub(a.d, 2) AND a.d
    ),
    pop AS (        -- популярность из dataai_usage
      SELECT CAST(product_id AS STRING) AS app_id,
             COALESCE(est_install_base, est_average_active_users) AS popularity
      FROM prod_atlas_datalake.ignite_silver.dataai_usage
      WHERE device_code = 'all'
        AND COALESCE(est_install_base, est_average_active_users) IS NOT NULL
    )
    SELECT n.user_id,
           AVG(p.popularity) AS user_avg_app_popularity
    FROM now n
    JOIN pop p ON n.app_id = p.app_id
    GROUP BY n.user_id
    """)
