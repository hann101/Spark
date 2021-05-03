import myspark
from myspark import spark as spark 

from pyspark.sql import functions as F

@F.udf('string')
def cat25(x):
    # 약속하고 만들어야한다. 
    if x < 100: return '100cm미만'
    elif x < 125: return '100~125cm'
    elif x < 150: return '125~150cm'
    elif x < 175: return '150~175cm'
    elif x < 200: return '175~200cm'
    else: return '200cm 이상'

df = spark.read.csv('./data/med.csv',header=True)
df = df.withColumn('hieght',cat25(F.col('신장')))
df = df.drop('신장').withColumnRenamed('hieght','신장')