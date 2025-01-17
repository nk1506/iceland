from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

import os

REST_CATALOG_URL = os.getenv("REST_CATALOG_URL")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

print(f"Catalog Configs: {REST_CATALOG_URL}, {S3_BUCKET}, {S3_ENDPOINT}")

try:
    # Initialize the Flink streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)


    print("Creating ReST catalog with Flink for Iceberg table")

    # Create the REST catalog
    table_env.execute_sql(f"""
        CREATE CATALOG rest_catalog WITH (
            'type'='iceberg',
            'catalog-type'='rest',
            'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
            'uri'='{REST_CATALOG_URL}',
            'warehouse'='{S3_BUCKET}',
            's3.endpoint'='{S3_ENDPOINT}'
        )
    """)

    # Use the REST catalog
    table_env.execute_sql("USE CATALOG rest_catalog")
    print(f"Current catalog: {table_env.get_current_catalog()}")

    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS flink;")
    table_env.use_database("flink")
    print(f"Current database: {table_env.get_current_database()}")

    # Create the Iceberg table
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS test_positional_merge_on_read (
            dt  date,
            number INTEGER,
            letter STRING
        ) WITH (
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'format-version'='2'
        )
    """)

    print("Table created")

    # Insert data into the Iceberg table
    table_env.execute_sql("""
    INSERT INTO test_positional_merge_on_read
    VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
    """)


    #  Create a table, and do some renaming
    table_env.execute_sql("CREATE OR REPLACE TABLE test_rename_column (lang string)")
    table_env.execute_sql("INSERT INTO test_rename_column VALUES ('Python')")
    # table_env.execute_sql("ALTER TABLE test_rename_column RENAME COLUMN lang TO language")
    table_env.execute_sql("INSERT INTO test_rename_column VALUES ('Java')")

    #  Create a table, and do some evolution
    table_env.execute_sql("CREATE OR REPLACE TABLE test_promote_column (foo int)")
    table_env.execute_sql("INSERT INTO test_promote_column VALUES (19)")
    # table_env.execute_sql("ALTER TABLE test_promote_column ALTER COLUMN foo TYPE bigint")
    table_env.execute_sql("INSERT INTO test_promote_column VALUES (25)")

    print("Finished provisioning tables")

except Exception as e:
    print(f"An error occurred: {e}")