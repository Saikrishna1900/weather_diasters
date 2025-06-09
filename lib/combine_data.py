from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from datetime import datetime

# Start Spark
spark = SparkSession.builder.appName("CombineWeatherDisasters").getOrCreate()

# Today's date
today = datetime.today().strftime('%Y%m%d')

# Paths
weather_path = f"/home/saiki_fdiq7b5/datalake/formatted/weather/{today}/weather.parquet"
disasters_path = f"/home/saiki_fdiq7b5/datalake/formatted/disasters/{today}/disasters.parquet"
output_path = f"/home/saiki_fdiq7b5/datalake/combined/{today}/combined.parquet"

# Read Parquet files
weather_df = spark.read.parquet(weather_path)
disasters_df = spark.read.parquet(disasters_path)

# Extract year and month from `time` column
weather_df = (
    weather_df
    .withColumn("year_str", col("time").substr(1, 4))
    .withColumn("month_number", col("time").substr(6, 2).cast("int"))
)

# Convert month names to numbers
disasters_df = (
    disasters_df
    .withColumn("month_number", when(col("Month") == "January", 1)
                 .when(col("Month") == "February", 2)
                 .when(col("Month") == "March", 3)
                 .when(col("Month") == "April", 4)
                 .when(col("Month") == "May", 5)
                 .when(col("Month") == "June", 6)
                 .when(col("Month") == "July", 7)
                 .when(col("Month") == "August", 8)
                 .when(col("Month") == "September", 9)
                 .when(col("Month") == "October", 10)
                 .when(col("Month") == "November", 11)
                 .when(col("Month") == "December", 12))
)

# Rename to avoid column conflict during join
disasters_df = disasters_df.withColumnRenamed("month_number", "disaster_month_number")

# Join on year and month
combined_df = weather_df.join(
    disasters_df,
    (weather_df.year_str == disasters_df.Year) &
    (weather_df.month_number == disasters_df.disaster_month_number),
    how="left"
)

# Save combined result
combined_df.write.mode("overwrite").parquet(output_path)

print(f"✅ Combined file saved to: {output_path}")
