from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Initialize the Flink streaming environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Environment variables
import sys
import os

REST_CATALOG_URL = os.getenv("REST_CATALOG_URL")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

print(f"Catalog Configs: {REST_CATALOG_URL}, {S3_BUCKET}, {S3_ENDPOINT}")

print("Starting gdelt tables for Flink")
# Create the REST catalog
table_env.execute_sql(f"""
    CREATE CATALOG rest_gdelt_catalog WITH (
            'type'='iceberg',
            'catalog-type'='rest',
            'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
            'uri'='{REST_CATALOG_URL}',
            'warehouse'='{S3_BUCKET}',
            's3.endpoint'='{S3_ENDPOINT}'
        )
""")

# Use the REST catalog
table_env.execute_sql("USE CATALOG rest_gdelt_catalog")
print(f"Current catalog: {table_env.get_current_catalog()}")

table_env.execute_sql("CREATE DATABASE IF NOT EXISTS flink;")
table_env.use_database("flink")
print(f"Current database: {table_env.get_current_database()}")

# Create the Iceberg table
table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS gdlet_table (
    id string,
    int_date int,
    year_month int,
    int_year int,
    decimal_year int,
    country string,
    city string,
    location_country string,
    field1 string,
    field2 string,
    field3 string,
    field4 string,
    field15 string,
    field16 string,
    field17 string,
    field18 string,
    field19 string,
    company_code string,
    company_city string,
    company_country string,
    field5 string,
    field6 string,
    field7 string,
    field8 string,
    code string,
    type string,
    sub_type string,
    category string,
    int_value int,
    value_type double,
    metric double,
    count1 int,
    count2 int,
    score double,
    int_rank int,
    country_name string,
    country_code string,
    region_code string,
    latitude double,
    longitude double,
    abbreviation string,
    count3 int,
    country_name2 string,
    country_code2 string,
    region_code2 string,
    latitude2 double,
    longitude2 double,
    abbreviation2 string,
    count4 string,
    country_name3 string,
    country_code3 string,
    region_code3 string,
    latitude3 double,
    longitude3 double,
    abbreviation3 string,
    count5 int,
    publish_date int,
    url string
    ) WITH (
        'format-version'='2',
        'write.format.default'='parquet'
    )
""")

print("Table created")

# Access command-line arguments
for arg in sys.argv[1:]:
    csv_file_path = f"{arg}.export.CSV"
    print(f"Provided csv file: {csv_file_path}")
    # Create the source table to read the GDELT CSV file
    table_env.execute_sql(f"""
        CREATE TEMPORARY TABLE temp_table (
        id string,
        int_date int,
        year_month int,
        int_year int,
        decimal_year int,
        country string,
        city string,
        location_country string,
        field1 string,
        field2 string,
        field3 string,
        field4 string,
        field15 string,
        field16 string,
        field17 string,
        field18 string,
        field19 string,
        company_code string,
        company_city string,
        company_country string,
        field5 string,
        field6 string,
        field7 string,
        field8 string,
        code string,
        type string,
        sub_type string,
        category string,
        int_value int,
        value_type double,
        metric double,
        count1 int,
        count2 int,
        score double,
        int_rank int,
        country_name string,
        country_code string,
        region_code string,
        latitude double,
        longitude double,
        abbreviation string,
        count3 int,
        country_name2 string,
        country_code2 string,
        region_code2 string,
        latitude2 double,
        longitude2 double,
        abbreviation2 string,
        count4 string,
        country_name3 string,
        country_code3 string,
        region_code3 string,
        latitude3 double,
        longitude3 double,
        abbreviation3 string,
        count5 int,
        publish_date int,
        url string
        ) WITH (
            'connector'='filesystem',
            'path'='{csv_file_path}',
            'format'='csv',
            'csv.field-delimiter' = '\t',
            'csv.line-delimiter' = '\n'
        )
    """)

    # Insert data from the source table into the Iceberg table
    result_table = table_env.execute_sql("""
        INSERT INTO gdlet_table SELECT * FROM temp_table
    """)


