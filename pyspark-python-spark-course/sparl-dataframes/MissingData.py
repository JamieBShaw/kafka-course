from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName('MissingData').getOrCreate()

df = spark.read.csv('ContainsNull.csv', inferSchema=True, header=True)

# Functional DataFrame Syntax

df.dropna()
df.dropna(thresh=2)
df.fillna('NA', subset=['Name']).show()

mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val[0][0]

df.fillna(mean_sales, ['Sales']).show() # Replaces NA values with mean values

# SQL Syntax
df.createOrReplaceTempView('missing')

