import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.sensors.external_task_sensor import ExternalTaskSensor
import psycopg2
from psycopg2 import Error
from psycopg2.extras import Json
import requests
from airflow.utils.dates import days_ago


default_args = {'owner': 'Dylan Bragdon', 'tags': ['data_collection']}
post_fmt = r"https://hacker-news.firebaseio.com/v0/item/{}.json?print=pretty"
newest_id_fmt = r"https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty"

def get_historic_posts(n: int):
    try:
        conn = psycopg2.connect(
            dbname = 'hackernews',
            user = 'postgres',
            host = 'host.docker.internal',
            password = 'postgres'
        )

        cur = conn.cursor()


        cur.execute("SELECT MIN(info->>'id')::int FROM post_json")
        oldest = cur.fetchone()[0]
        #if no posts are in post_json, start from newest post
        if not oldest:
            r = requests.get(newest_id_fmt)
            oldest = r.json()
            r = requests.get(post_fmt.format(oldest))
            obj = r.json()
            cur.execute("INSERT INTO post_json (info) VALUES (%s)",
                        [Json(obj)])
            n -= 1
        #starting from oldest post, count backwards from n
        for i in range(n):
            oldest -= 1
            r = requests.get(post_fmt.format(oldest))
            obj = r.json()
            cur.execute("INSERT INTO post_json (info) VALUES (%s)",
                        [Json(obj)])
        conn.commit()
        conn.close()

    except Error as E:
        print(E)

def get_new_posts():
    try:
        conn = psycopg2.connect(
            dbname = 'hackernews',
            user = 'postgres',
            host = 'host.docker.internal',
            password = 'postgres'
        )

        cur = conn.cursor()


        #get most recent saved id in postgres
        cur.execute("SELECT MAX(info->>'id')::int FROM post_json")
        max_saved_post = cur.fetchone()[0]

        #get newest id from API
        newest_id = requests.get(newest_id_fmt).json()

        #if no posts are in post_json, insert newest post on API
        if not max_saved_post:
            r = requests.get(post_fmt.format(newest_id))
            newest = r.json()
            cur.execute("INSERT INTO post_json (info) VALUES (%s)",
                        [Json(newest)])

        #if max post IS newest post on API, do nothing
        elif max_saved_post == newest_id:
            print('Up to date.')

        #if newest post in post_json is not up to date
        else:
            n = newest_id - max_saved_post
            curr_id = newest_id
            print('Fetching {} new posts.'.format(n))
            for i in range(n):
                    r = requests.get(post_fmt.format(curr_id))
                    curr_data = r.json()
                    cur.execute("INSERT INTO post_json (info) VALUES (%s)",
                                [Json(curr_data)])
                    curr_id -= 1

        conn.commit()
        conn.close()

    except Error as E:
        print(E)

#DAGS
historic_post_dag = DAG(
    dag_id = "historic_post_collection",
    default_args = default_args,
    description = "Get historic post data",
    schedule_interval = None,
    start_date = days_ago(2)
)


new_post_dag = DAG(
    dag_id = 'new_post_collection',
    default_args = default_args,
    description = "Catchup to latest post",
    schedule_interval = "* * * * *",           #runs every minute
    catchup = False,
    start_date = days_ago(2)
)

#TASKS
get_historic_posts_task = PythonOperator(
                        task_id = 'get_historic_posts',
                        python_callable = get_historic_posts,
                        op_kwargs = {'n': 200},
                        dag = historic_post_dag)

get_new_posts_task = PythonOperator(
        task_id = 'get_new_posts',
        python_callable = get_new_posts,
        dag = new_post_dag)
