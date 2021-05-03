import myspark
from myspark  import spark as spark


DATA_ROOT = './data'

ROW_ID_COLUMN = 'rowid'

customers = ['fin','med']

def read_csv(path, customer):
    df = spark.read.csv(path, header=True)
    new_names = [f'{customer}_{colname}'for colname in df.columns]
    # 하나하나 컬럼 앞에 붙일 것이다.
    df = df.toDF(*new_names)
    # 리스트로 묶어 주궸다.새로운 이름으로 바꾸긴 하는데, a1,b1 정도로 바꿔주는것.
    # 컬럼이름을 가져온 그대로 하고 있다. 모든 컬럼명을 일괄적으로 바꿀 때 좋다. 
    return df

df = spark.read.csv(f'{DATA_ROOT}/joined.key', header=True)
# ㅇ읽어서 각 회사의 데이터를 읽는다. 

counts = {}
for customer in customers:
    dfdata = read_csv(f'{DATA_ROOT}/join/{customer}.data',customer)
    counts[customer] = dfdata.count()
    df = df.join(dfdata, on = [f'{customer}_{ROW_ID_COLUMN}'], how='left')
    # 데이터를 결합하기 때문에 rowid를 합치도록한다. 
    # a회사의 것만 결합한다. 양쪽에 데이터가있으..
    # 왜 left join? join된 키는 a회사의 데이터가 없는 것들이 있음.. 없는 row들을 살리기 위한것.
    # 데이터가 이미 있어도 결합할 대상이 없기 때문에 

df = df.drop(*[f'{customer}_{ROW_ID_COLUMN}'for customer in customers])
# row데이터를 합치는 과정..
df.write.mode('overwrite').csv(f'{DATA_ROOT}/join/joined.data', header=True)

count_joined = df.count()
for customer in customers:
    print(f'\t{customer}:{count_joined}/{counts[customer]}:{count_joined/counts[customer]}')