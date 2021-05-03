import myspark
from myspark import spark as spark 
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256



DATA_ROOT = './data'
JOIN_KEY_COLUMN = 'joinkey'
ROW_ID_COLUMN = 'idx'
customers = ['2','3']
def read_csv(path,customer):
    df = spark.read.csv(path, header=True)
    df = df.withColumnRenamed(ROW_ID_COLUMN,f'{customer}_{ROW_ID_COLUMN}')
    return df

counts = {}

df = read_csv(f'{DATA_ROOT}/{customers[0]}_keytable.csv', customers[0])
counts[customers[0]]= df.count()
# 2조 3조에서 만들어준 key의 이름을 2_keytable, 3_keytable로 저장하여 사용했습니다.

for customer in customers[1:]:
    dfnext = read_csv(f'{DATA_ROOT}/{customer}_keytable.csv',customer)
    counts[customer]= dfnext.count()
    df = df.join(dfnext, on = [JOIN_KEY_COLUMN],how='inner')

df = df.select(*[f'{customer}_{ROW_ID_COLUMN}' for customer in customers])
df.coalesce(1).write.mode('overwrite').csv(f'{DATA_ROOT}/join_0/joined.key',header=True)

count_joined = df.count()
for customer in customers:
    print(f'\t{customer}:{count_joined}/{counts[customer]}:{count_joined/counts[customer]}')
