# Exercise 4 of Exercises with Spark SQL
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col

spark = SparkSession.builder.appName("App").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema_fields = ["project_name", "page_title", "num_requests", "content_size"]

inputDf = spark.read.format("csv").option("delimiter", " ").load("pagecounts-20100806-030000").toDF(*schema_fields)

print("project_summary table")
project_summary = inputDf.groupBy("project_name").agg(sum("num_requests").alias("num_pages"),
                                                      sum("content_size").alias("total_content_size"),
                                                      avg("num_requests").alias("mean_requests"))

project_summary.show()

print("most_visited table")
most_visited = inputDf.join(
    project_summary,
    on="project_name"
).filter(
    col("num_requests") > col("mean_requests")
).select(
    schema_fields
)

most_visited.show()