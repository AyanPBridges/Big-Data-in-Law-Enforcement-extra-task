from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lower, split
spark = SparkSession.builder.appName("task2").getOrCreate()
text_df = spark.read.text('kazakhstan_law_enforcement_law.txt')
words_df = text_df.select(explode(split(lower(text_df.value), "\\W+")).alias("word"))
words_filtered_df = words_df.filter(words_df.word != "")

counted_df = words_filtered_df.groupBy("word").count()
counted_sort = counted_df.sort(counted_df["count"].desc())
counted_sort.show()
