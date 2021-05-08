from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'Dylan Bragdon',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['vaccine'])
def get_vaccine_data():

    @task()
    def load_df():
        url = r'https://opendata.arcgis.com/datasets/da83fdaab14e42f0b3fe198a15c5bad5_0.csv?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D'
        data = pd.read_csv(url)
        print(data.head)

    load_df()

run = get_vaccine_data()
