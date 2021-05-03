import myspark
from myspark import spark as spark

from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header=True)

# 불연속 & 
df = df.withColumn('idx1', F.monotonically_increasing_id())

df = df.withColumn('idx2', F.row_number().over(W.orderBy('idx1')))
df.select('idx2','일련번호').show()
df.select(F.min('idx1'),F.max('idx2'),F.max('idx2')).show()