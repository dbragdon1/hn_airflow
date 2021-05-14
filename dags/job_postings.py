from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
from psycopg2 import Error
from psycopg2.extras import Json
import requests

default_args = {'owner': 'Dylan Bragdon', 'tags': ['data_collection']}
post_fmt = r"https://hacker-news.firebaseio.com/v0/item/{}.json?print=pretty"
newest_id_fmt = r"https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty"


def process_request(id):
    url = post_fmt.format(id)
    r = requests.get(url)
    data = r.json()
    return data

def most_recent_job_ids():
    url = "https://hacker-news.firebaseio.com/v0/jobstories.json?print=pretty"
    r = requests.get(url)
    ids = r.json()
    for id_ in ids:
        data = process_request(id_)
        print(data)
        break
most_recent_job_ids()
