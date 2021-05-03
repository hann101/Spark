import myspark
from myspark import spark as spark
import uuid

from pyspark.sql import functions as F
df  = spark.read.csv('./data/fin.csv', header = True)
df = df.withColumn('idx',F.lit(str(uuid.uuid4())))

df.select('idx','일련번호').show(truncate=False)