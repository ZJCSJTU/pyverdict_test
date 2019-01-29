import random
import pymysql
import pyverdict
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    dbname='postgres',
    password=''
)

conn.autocommit = True

cur = conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS testschema CASCADE')
cur.execute('CREATE SCHEMA IF NOT EXISTS testschema')
cur.execute('DROP TABLE IF EXISTS testschema.sale')
#cur.execute('CREATE DATABASE myschema')
cur.execute(
    'CREATE TABLE testschema.sale (' +
    '   product varchar(100),' +
    '   price  float)'
)


# insert 100 rows
product_list = ['milk', 'egg', 'juice']
random.seed(0)
for i in range(10000):
    rand_idx = random.randint(0, 2)
    product = product_list[rand_idx]
    price = (rand_idx + 2) * 10 + random.randint(0, 10)
    cur.execute(
        'INSERT INTO testschema.sale (product, price)' +
        '   VALUES (\'{:s}\', {:f})'.format(product, price)
    )


# sql using cur
cur.execute(
    "SELECT product, AVG(price) " +
    "FROM testschema.sale " +
    "GROUP BY product " +
    "ORDER BY product")
df = cur.fetchall()
print(df)


cur.close()

# create connection
verdict_conn = pyverdict.postgres(
    host='localhost',
    user='postgres',
    dbname='postgres',
    password='',
    port=5432
)

# create scramble table

verdict_conn.sql('CREATE SCRAMBLE testschema.sale_scrambled from testschema.sale')

# run query
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM testschema.sale " +
    "GROUP BY product " +
    "ORDER BY product")
print(df)
