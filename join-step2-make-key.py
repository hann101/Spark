import myspark
from myspark import spark as spark 
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256



DATA_ROOT = './data'
JOIN_KEY_COLUMN = 'joinkey'
ROW_ID_COLUMN = 'rowid'
customers = ['fin','med']
def read_csv(path,customer):
    df = spark.read.csv(path, header=True)
    df = df.withColumnRenamed(ROW_ID_COLUMN,f'{customer}_{ROW_ID_COLUMN}')
    return df

counts = {}
# 중괄호하는이유 = 리스트/배열은 순서를 찾는 (인덱스 접근) 반면, 
# 데이터가 많아지면 인덱싱하는 것 조차 힘듦. 
# 근데 데이터의 단어 자체가 인덱스 역할을 한다. 


df = read_csv(f'{DATA_ROOT}/join/{customers[0]}.key', customers[0])
counts[customers[0]]= df.count()

for customer in customers[1:]:
    dfnext = read_csv(f'{DATA_ROOT}/join/{customer}.key',customer)
    counts[customer]= dfnext.count()
    df = df.join(dfnext, on = [JOIN_KEY_COLUMN],how='inner')

df = df.select(*[f'{customer}_{ROW_ID_COLUMN}' for customer in customers])
df.write.mode('overwrite').csv(f'{DATA_ROOT}/join/joined.key',header=True)

count_joined = df.count()
for customer in customers:
    print(f'\t{customer}:{count_joined}/{counts[customer]}:{count_joined/counts[customer]}')
