# Import PySpark and create a Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('example_app').master('yarn').getOrCreate()

# Read a text file from HDFS
textFile = spark.read.text("hdfs://...")

# Count the number of lines in the file
numLines = textFile.count()
print(f"The file has {numLines} lines.")

# Filter the lines that contain the word "error"
errors = textFile.filter(textFile.value.contains("error"))

# Count the number of errors and print some examples
numErrors = errors.count()
print(f"The file has {numErrors} errors.")
errors.show(truncate=False)