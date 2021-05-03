
import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import math

df = spark.read.csv('./data/med.csv', header=True)

QI = [
    'birth_year',
    '시도',
    '성별',
    'height_2',
    'weight_2',
    'ding_year',
    'in_year',
    'out_year',
    '흡연상태',
    '음주여부'
]
A = df.groupby(*QI).count().filter('count = 1').count()

for ii in range(0,len(Q)):
    qi = QI[:ii] +QI[ii+1]