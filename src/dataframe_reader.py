from pyspark.sql import SparkSession

# Create a SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

# Path to data set
file = "../data/2010-summary.parquet"

# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = spark.read.format("parquet").load(file)
df.show(4, truncate=False)
