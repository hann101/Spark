import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
DATA_ROOT ='./data'
df2 =spark.read.csv('./data/2조_결합키.csv', header = True)

# df.select('고객등급').distinct.show()



# df = df.withColumn('income_avg',df['income_avg'].cast('int'))

# df = df.select('고객등급','income_avg').show(truncate = False)
# df1 = df.groupby('고객등급').agg(F.mean('income_avg').alias('mean'))
# # 이름을 기여를 한다. 
# df1.show()

# df2 = df.join(df1, on = ['고객등급'], how = 'left')
# df2 = df2.drop('income_avg')
# # drop 컬럼삭제 , 
df2 = df2.withColumnRenamed('hash','joinkey')

df2.select('joinkey').show(truncate = True)
# 필요한 컬럼을 selecet를 ㄹ사용해서 넣으면 된다. 
df2.write.mode('overwrite').csv(f'{DATA_ROOT}/join_3/joined.key',header=True)