def user_avg_app_popularity(
    app_catalog_table: str = "prod_atlas_datalake.ignite_silver.app_catalog_reporting_vw",
    company_apps_table: str = "prod_one_dt_datalake.lakehouse_reporting.dataai_dim_company_apps",
    date: str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    window: int = 30,
    country: str = "br",
    partner: str = "motorola",
    output_table_name: str = "user_avg_app_popularity_30d",
) -> DataFrame:
    """
    Calculates average popularity of apps per user over the last `window` days.
    Minimal fixes:
      - aggregate popularity across all device_code (no 'all' filter)
      - bridge join: package_name -> product_id via dataai_dim_company_apps
    """
    df_out = spark.sql(f"""
    WITH base AS (
      SELECT
        date,
        hashed_aaid            AS user_id,
        package_name           AS app_id
      FROM {app_catalog_table}
      WHERE date BETWEEN date_sub('{date}', {window}) AND '{date}'
        AND NOT prod_atlas_datalake.common_functions.iszeroaaid(hashed_aaid)
        AND app_catalog_schedule IN ('24hr','Heart_Beat')
        AND is_system_package = false
        AND LOWER(country_code) = LOWER('{country}')
        AND partner = '{partner}'
        AND package_name IS NOT NULL
    ),
    anchor_u AS (
      SELECT user_id, MAX(date) AS d
      FROM base
      GROUP BY user_id
    ),
    now AS (
      SELECT DISTINCT b.user_id, b.app_id
      FROM base b
      JOIN anchor_u a
        ON b.user_id = a.user_id AND b.date = a.d
    ),

    -- Bridge: product_slug -> product_id
    bridge AS (
      SELECT LOWER(product_slug) AS pkg, CAST(product_id AS STRING) AS product_id
      FROM {company_apps_table}
      WHERE product_slug IS NOT NULL AND product_id IS NOT NULL
      -- for duplicates in slug
      QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(product_slug) ORDER BY product_id) = 1
    ),

    -- Popularity aggregated over ALL device_code (эквивалент 'all')
    pop AS (
      SELECT
        CAST(product_id AS STRING) AS app_id,
        SUM(COALESCE(est_install_base, est_average_active_users)) AS popularity
      FROM prod_one_dt_datalake.lakehouse_reporting.dataai_usage
      WHERE COALESCE(est_install_base, est_average_active_users) IS NOT NULL
      GROUP BY product_id
    )

    SELECT
      n.user_id,
      AVG(p.popularity) AS user_avg_app_popularity
    FROM now n
    JOIN bridge b
      ON LOWER(n.app_id) = b.pkg
    JOIN pop p
      ON p.app_id = b.product_id
    GROUP BY n.user_id
    """)

    df_out.createOrReplaceTempView(output_table_name)
    spark.sql(f"DROP TABLE IF EXISTS prod_atlas_datalake.datascience_sandbox.{output_table_name}")
    spark.sql(f"""
      CREATE TABLE prod_atlas_datalake.datascience_sandbox.{output_table_name} AS
      SELECT * FROM {output_table_name}
    """)
    return df_out
