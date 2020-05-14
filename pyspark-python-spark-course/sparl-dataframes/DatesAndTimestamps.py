from pyspark.sql import SparkSession
from pyspark.sql.functions import (dayofmonth, hour,
                                   dayofyear, month,
                                   year, weekofyear,
                                   format_number, date_format)

spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)


#df.select(['Date', 'Open']).show()df.take(20)

# Date Formatting

df.select(dayofmonth(df['Date']))
df.select(month(df['Date']))
df.select(year(df['Date']))
df.select(hour(df['Date']))


df_year = df.withColumn('Year', year(df['Date']))
df_year_avg = df_year.groupBy("Year").mean().select(["Year", "avg(Close)"])
result = df_year_avg.withColumnRenamed("avg(Close)", "Average Closing Price")
new = result.select(['Year', format_number('Average Closing Price', 2).alias("Average Close")])
new.show()






