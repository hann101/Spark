import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import math

DATA_ROOT = './data'

round__ = F.udf(lambda x: round(x, -len(str(x))+2))

df = spark.read.csv(f'{DATA_ROOT}/balance/finance_data/*.csv', header=True)
df = df.withColumn('permit_new_credit_balance', df['permit_new_credit_balance'].cast('int'))
df = df.withColumn('housing_new_balance', df['housing_new_balance'].cast('int'))
df = df.withColumn('income_avg', df['income_avg'].cast('int'))
df = df.withColumn('internet_bank_balance', df['internet_bank_balance'].cast('int'))
# int형으로 변환을 함.

df = df.withColumn('permit_new_credit_balance', round__(F.col('permit_new_credit_balance')))
df = df.withColumn('housing_new_balance', round__(F.col('housing_new_balance')))
df = df.withColumn('income_avg', round__(F.col('income_avg')))
df = df.withColumn('internet_bank_balance', round__(F.col('internet_bank_balance')))
# round__함수를 사용해서 반올림을 함.

df.coalesce(1).write.mode('overwrite').csv('./data/balance/finance_anonymity', header=True)