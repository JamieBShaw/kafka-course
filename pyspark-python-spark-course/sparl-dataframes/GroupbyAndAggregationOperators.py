from pyspark.sql import SparkSession
from pyspark.sql.functions import (countDistinct, avg, stddev, round, format_number)

spark = SparkSession.builder.appName('groupbyAndAggregations').getOrCreate()

df = spark.read.csv('sales_info.csv', inferSchema=True, header=True)

# All the below are different syntax but the same output

# Functional DataFrame Syntax

df.groupBy('Company').sum('Sales')
# missing
df.agg({'Sales': 'sum'})
df.select(round(avg('Sales'), 2).alias('Average Sales'))
df.select(round(stddev('Sales'), 2).alias('Sales Std'))
df.orderBy(df['Sales'].desc())


# SQL SYNTAX
df.createOrReplaceTempView('salesInfo')

results_company_sum = spark.sql("SELECT Company, SUM(sales) FROM salesInfo GROUP BY Company")
results_company_people = spark.sql("SELECT Company, Person FROM salesInfo WHERE Sales > 200")
results_sum = spark.sql("SELECT SUM(sales) as totalSales FROM salesInfo")
results_round_avg = spark.sql('SELECT ROUND(AVG(sales), 2) as averageSales FROM salesInfo')
# results_round_std = spark.sql('SELECT ROUND(STDEV(Sales]) as [salesSTD] FROM salesInfo')  # STDEV not working
results_orderBy_desc = spark.sql('SELECT * FROM salesInfo ORDER BY Sales DESC ')
results_orderBy_desc.show()
