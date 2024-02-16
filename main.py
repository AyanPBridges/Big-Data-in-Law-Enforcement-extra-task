from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TASK3").getOrCreate()
path = 'Census_Data.csv'
df = spark.read.csv(path, inferSchema=True, header=True)
rdd = df.rdd
top3comm = rdd.top(3, key=lambda x: x['HARDSHIP INDEX'] if x['HARDSHIP INDEX'] is not None else 0)
total = sum([community['PER CAPITA INCOME '] for community in top3comm])

print(f"top 3 communities total per capita income num: {total}")

# print(df.columns)