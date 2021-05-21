from pyspark.sql import SparkSession

# Define schema for our data using DDL
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY < STRING > "

# Create our static data
data = [
    [1, "Jules", "Damji", "https://example.com/1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
    [2, "Brooke", "Wenig", "https://example.com/2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
    [3, "Denny", "Lee", "https://example.com/3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
    [4, "Tathagata", "Das", "https://example.com/4", "5/12/2018", 10568, ["twitter", "FB"]],
    [5, "Matei", "Zaharia", "https://example.com/5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
    [6, "Reynold", "Xin", "https://example.com/6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
]

# Main program
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Example-3_6")
             .getOrCreate())

    # 予め列挙したデータとDDLでのSchema定義を元に DataFrameを作る
    blogs_df = spark.createDataFrame(data, schema)

    blogs_df.show()
    print(blogs_df.printSchema())
    print(blogs_df.schema)
