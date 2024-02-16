from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("PoliceFatalityAnalysis").getOrCreate()
path = "/Users/ospanov/PycharmProjects/pythonProject1/police_fatality_without_header.csv"

schema = StructType([
    StructField("name", StringType(), True),
    StructField("manner_of_death", StringType(), True),
    StructField("date", StringType(), True),
    StructField("state", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("empty1", IntegerType(), True),
    StructField("empty2", IntegerType(), True),
    StructField("empty3", IntegerType(), True),
    StructField("empty4", IntegerType(), True)
])

df = spark.read.csv(path, schema=schema, header=False)
male_result = df.filter(df.gender == 'Male').count() + df.filter(df.gender == 'male').count()
print(f"male victims: {male_result}")