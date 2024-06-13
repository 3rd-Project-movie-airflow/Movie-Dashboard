from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import urllib.request
import json
import pandas as pd
import re

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def remove_b_tags(text):
    return re.sub(r'<\/?b>', ' ', text)

def make_clickable(link):
    return f'<a href="{link}" target="_blank">Link</a>'

def etl():
    url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
    params = {
        'key': Variable.get("movie_api_key_3"),
        'targetDt': (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    }
    res = requests.get(url, params=params)
    data = res.json()
    temp = data['boxOfficeResult']['dailyBoxOfficeList']
    movie_list = [movie['movieNm'] for movie in temp]

    client_id = Variable.get("NAVER_CLIENT_ID")
    client_secret = Variable.get("NAVER_CLIENT_SECRET")

    news_data = []

    for movie in movie_list:
        encText = urllib.parse.quote(movie)
        url = f"https://openapi.naver.com/v1/search/news.json?query={encText}&display=1&sort=sim"
        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)
        response = urllib.request.urlopen(request)
        response_body = response.read()
        response_json = json.loads(response_body.decode('utf-8'))
        items = response_json.get("items", [])
        if items:
            news_data.append(items[0])
        else:
            print(f"No news found for movie: {movie}")

    df = pd.DataFrame(news_data)
    df['title'] = df['title'].apply(remove_b_tags)
    df['link'] = df['link'].apply(make_clickable)
    df['movie_title'] = movie_list
    df['rank'] = df.index + 1
    df = df[['rank', 'movie_title', 'title', 'link']]

    cur = get_Redshift_connection()

    cur.execute("""
    DROP TABLE IF EXISTS zkvmek963.boxoffice_news;
    CREATE TABLE zkvmek963.boxoffice_news (
        rank int,
        movie_title varchar(256) PRIMARY KEY,
        news_title varchar(512),
        link varchar(512)
    );
    """)

    for index, row in df.iterrows():
        rank = row['rank']
        movie_title = row['movie_title']
        news_title = row['title'].replace("'", "''")
        link = row['link']
        sql = f"INSERT INTO zkvmek963.boxoffice_news VALUES ({rank}, '{movie_title}', '{news_title}', '{link}')"
        cur.execute(sql)

    cur.close()

dag = DAG(
    'boxoffice_news',
    description='Fetch box office data and insert into Redshift',
    start_date=datetime(2024, 6, 9, 12, 0),
    schedule_interval='0 12 * * *',
    catchup=False
)

boxoffice_news_task = PythonOperator(
    task_id='boxoffice_news_task',
    python_callable=etl,
    provide_context=True,
    dag=dag,
)

boxoffice_news_task
