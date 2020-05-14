from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StringType,
                                IntegerType, StructField)

spark = SparkSession.builder.appName('Basics').getOrCreate()

data_scheme =[StructField('age', IntegerType(), True),
              StructField('name', StringType(), True)]

final_struc = StructType(data_scheme)

df = spark.read.json('people.json', schema=final_struc)
# df.show()
# df.printSchema()
# df.describe().show()

df.select('age')
df.head(2)
df.withColumn('newAge', df['age']).show()

df.withColumnRenamed('age', 'my_new_age').show()

df.createOrReplaceTempView('people')

results = spark.sql("SELECT * FROM people WHERE age=30")

