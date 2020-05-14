from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)

# df.filter("close < 500").select(['open', 'close']).show()

# df.filter((df['Close'] < 200) & (df['Open'] > 200)).show()

# results = df.filter(df['low'] == 197.16).collect()