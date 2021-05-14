import psycopg2
import json
import requests
from psycopg2.extras import Json



try:

    conn = psycopg2.connect(dbname = 'hackernews',
                            user = 'postgres',
                            host = 'localhost',
                            password = 'postgres')
    cur = conn.cursor()
except:
    print('Connection Failed')

try:

    r = requests.get('https://hacker-news.firebaseio.com/v0/item/2921983.json?print=pretty')
    obj = r.json()


    cur.execute("""
                INSERT INTO post_json(info) VALUES ({});
                """.format(r.json())
    )

    conn.commit()

    conn.close()

except psycopg2.Error as E:
    print(E)
    print('Write Failed')
