import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import math

df = spark.read.csv('./data/med.csv', header=True)
count_all = df.count()

year__ = F.udf(lambda x: x[:4], 'string')
df = df.withColumn('birth_year', year__(df['생년월일']))

df = df.withColumn('신장', df['신장'].cast('int'))
df = df.withColumn('체중', df['체중'].cast('int'))
cat_10__ = F.udf(lambda x: math.floor(x / 10) * 10, 'int')
df = df.withColumn('height', cat_10__(df['신장']))
df = df.withColumn('weight', cat_10__(df['체중']))

height_local__ = F.udf(lambda x: 190 if x > 190 else 130 if x < 130 else x, 'int')
weight_local__ = F.udf(lambda x: 110 if x > 100 else 30 if x < 40 else x, 'int')
df = df.withColumn('height_2', height_local__(df['height']))
df = df.withColumn('weight_2', weight_local__(df['weight']))

QI = ['birth_year', '시도', '성별', 'height_2', 'weight_2']

df1 = df.groupby(*QI).count()
dfx = df1.filter('count >= 3').join(df, on=QI, how='left')
#dfx.select(*QI, 'count').orderBy('count').show()

dfx = dfx.drop('count')

df2 = dfx.select(*QI, '진료과명').distinct().groupby(*QI).count()
#df2.orderBy('count').show()

df3 = df2.filter('count < 2')
#df3.join(df, on=QI, how='left').select(*QI, '진료과명').show()

#df3.join(df, on=QI, how='left').write.mode('overwrite').csv('./data/out-k3-l2', header=True)

dfl = dfx.join(df3, on=QI, how='left_anti')
dfl = dfl.drop('count')

dfl.coalesce(1).write.mode('overwrite').csv('./data/out-k-3-l2', header=True)
