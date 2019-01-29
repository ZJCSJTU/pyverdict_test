import random
import pymysql
import pyverdict

mysql_conn = pymysql.connect(
    host='localhost',
    port=3305,
    user='root',
    passwd='',
    autocommit=True
)
cur = mysql_conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS myschema')
cur.execute('CREATE SCHEMA myschema')
cur.execute(
    'CREATE TABLE myschema.sales (' +
    '   product varchar(100),' +
    '   price   double)'
)

# insert 1000 rows
product_list = ['milk', 'egg', 'juice']
random.seed(0)
for i in range(1000):
    rand_idx = random.randint(0, 2)
    product = product_list[rand_idx]
    price = (rand_idx + 2) * 10 + random.randint(0, 10)
    cur.execute(
        'INSERT INTO myschema.sales (product, price)' +
        '   VALUES ("{:s}", {:f})'.format(product, price)
    )

cur.close()

# create connection
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3305
)

# create scramble table
verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')

# run query
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product")

print(df)
