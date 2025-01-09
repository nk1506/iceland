# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("Starting gdelt tables")

# Spark initialization
spark = (
    SparkSession
        .builder
        .getOrCreate()
)

#Schema for csv files
schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("year_month", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("decimal_year", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("location_country", StringType(), True),
    StructField("field1", StringType(), True),  # Empty field
    StructField("field2", StringType(), True),  # Empty field
    StructField("field3", StringType(), True),  # Empty field
    StructField("field4", StringType(), True),  # Empty field
    StructField("field15", StringType(), True),  # Empty field
    StructField("field16", StringType(), True),  # Empty field
    StructField("field17", StringType(), True),  # Empty field
    StructField("field18", StringType(), True),  # Empty field
    StructField("field19", StringType(), True),  # Empty field
    StructField("company_code", StringType(), True),
    StructField("company_city", StringType(), True),
    StructField("company_country", StringType(), True),
    StructField("field5", StringType(), True),  # Empty field
    StructField("field6", StringType(), True),  # Empty field
    StructField("field7", StringType(), True),  # Empty field
    StructField("field8", StringType(), True),  # Empty field
    StructField("code", StringType(), True),
    StructField("type", StringType(), True),
    StructField("sub_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("value_type", DoubleType(), True),
    StructField("metric", DoubleType(), True),
    StructField("count1", IntegerType(), True),
    StructField("count2", IntegerType(), True),
    StructField("score", DoubleType(), True),
    StructField("rank", IntegerType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("abbreviation", StringType(), True),
    StructField("count3", IntegerType(), True),
    StructField("country_name2", StringType(), True),
    StructField("country_code2", StringType(), True),
    StructField("region_code2", StringType(), True),
    StructField("latitude2", DoubleType(), True),
    StructField("longitude2", DoubleType(), True),
    StructField("abbreviation2", StringType(), True),
    StructField("count4", IntegerType(), True),
    StructField("country_name3", StringType(), True),
    StructField("country_code3", StringType(), True),
    StructField("region_code3", StringType(), True),
    StructField("latitude3", DoubleType(), True),
    StructField("longitude3", DoubleType(), True),
    StructField("abbreviation3", StringType(), True),
    StructField("count5", IntegerType(), True),
    StructField("publish_date", IntegerType(), True),
    StructField("url", StringType(), True)
])

# Create an iceberg table with csv schema.
spark.sql(
    f"""
CREATE OR REPLACE TABLE rest.default.gdlet_table (
    id string,
    date int,
    year_month int,
    year int,
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
    value int,
    value_type double,
    metric double,
    count1 int,
    count2 int,
    score double,
    rank int,
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
)
USING iceberg;
"""
)

# Access command-line arguments
for arg in sys.argv[1:]:
    csv_file_path = f"{arg}.export.CSV"
    print(f"Provided csv file: {csv_file_path}")
    df = spark.read.csv(csv_file_path, schema=schema, sep='\t', header=False)
    # Create a temporary view (table) from the DataFrame
    df.createOrReplaceTempView("temp_table")
    # Insert data from the temporary table into the permanent table
    spark.sql("""
        INSERT INTO rest.default.gdlet_table
        SELECT * FROM temp_table
    """)

# Verify data insertion
print("Displaying few records")
spark.sql("SELECT * FROM rest.default.gdlet_table").show()

print("Total count of records")
spark.sql("SELECT count(*) as total_count FROM rest.default.gdlet_table").show()

print("Finished gdelt tables")
