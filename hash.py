import myspark
from myspark import spark as spark 
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256

DATA_ROOT = './data'
JOIN_KEY_COLUMN = 'joinkey'
# 컬럼을 고정시키고 상수처럼 사용
ROW_ID_COLUMN = 'rowid'
# 다시 사용할 수 없으니, 새로운 row를 만들어 주는 것이다. 
SALT = b'Salt is salty..'
# 고정
KEY_COLUMNS = ['sn']
# 튜플로 해야 안전하다.
# 구분자를 반드시 확인.. ,가 될 수 도 이쏘. 다른게 될 수 도 잇음. 

customers=['finance-210401']
@F.udf('string')
def hash(msg):
    h= SHA256.new(SALT)
    h.update(msg.encode('utf-8'))
    return h.hexdigest()

def make_key_file(customer):

    # 결합관리기관에 줄것 --------------------
    # key와 row를 만들어 주기 위한 함수이다. 
    df = spark.read.csv(f'{DATA_ROOT}/{customer}.csv',header=True)
    # df2 = df.drop('income_avg')
    df = df.withColumn(JOIN_KEY_COLUMN,hash(F.concat_ws('|', *KEY_COLUMNS)))
    # concat은 컬럼을 이어 붙이는 역할을 한다. |를 구분자로 해서 df를 만든다. 
    df = df.withColumn(ROW_ID_COLUMN,F.monotonically_increasing_id())
    # 결합시 row id는 의미 없음.. 구분만하면 된다. uuid를 사용하면 길어짐.. string으로 하기 때문에 크다. 이건 init이기 때문에 더 빠르다.
    df.write.mode('overwrite').csv(f'{DATA_ROOT}/join_2/{customer}.data',header = True)

    dfkey = df.select(JOIN_KEY_COLUMN,ROW_ID_COLUMN)
    dfkey.write.mode('overwrite').csv(f'{DATA_ROOT}/join_2/{customer}.key',header=True)

# 생년,주소,핸도폰,ㅅ성별,이름

    # 다른기관에 줄것 ------------------------------
    # 키를 만들때 쓴 컬럼은 빼야한다. 
    dfdata = df.drop(JOIN_KEY_COLUMN, *KEY_COLUMNS)
    # 여기서 지운다. 
    dfdata.write.mode('overwrite').csv(f'{DATA_ROOT}/join_2/{customer}.data',header=True)


for customer in customers:
    make_key_file(customer)