from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TASK3").getOrCreate()
path = 'AtlasofSurveillance.csv'
df = spark.read.csv(path, inferSchema=True, header=True)

res_df = df.filter((df['State'] == 'VA') & (df['Type of LEA'] == 'Police'))
count = res_df.count()

print(f"num of surveillance cameras where state VA and type of lea police: {count}")