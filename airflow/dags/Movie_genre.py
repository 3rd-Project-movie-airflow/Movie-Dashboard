from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests
import logging
import psycopg2
import math

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def fetch_movie_genre():
    api_key = Variable.get("movie_api_key_2")
    
    url = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={api_key}&openStartDt=2024&openEndDt=2024"
    res = requests.get(url)
    if res.status_code != 200:
        raise ValueError(f"API 호출 중 오류 발생: {res.status_code}")

    data = res.json()
    
    if 'movieListResult' not in data :
        raise ValueError(f"API 응답 구조가 예상과 다름 1: {data}")
    elif 'totCnt' not in data['movieListResult']:
        raise ValueError(f"API 응답 구조가 예상과 다름 2: {data}")
    
    cnt = data['movieListResult']['totCnt']

    repGenreNm = []

    # API 요청 페이지 수 계산
    pages = math.ceil(cnt / 100)

    for i in range(1, pages + 1):
        res = requests.get(f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={api_key}&openStartDt=2024&openEndDt=2024&curPage={i}&itemPerPage=100")
        if res.status_code != 200:
            raise ValueError(f"API 호출 중 오류 발생: {res.status_code}")
        
        data = res.json()
        for d in data['movieListResult']['movieList']:
            repGenreNm.append(d['repGenreNm'])
    
    return repGenreNm

@task
def etl(repGenreNm):
    cur = get_Redshift_connection()
    
    try:
        logging.info("ETL 시작")
        cur.execute("BEGIN;")
        
        cur.execute("""
            DROP TABLE IF EXISTS dev.zkvmek963.genre_2024;
        """)
        cur.execute("""
            CREATE TABLE dev.zkvmek963.genre_2024 (
            ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            Genre VARCHAR(255)
        )
        """)

        insert_query = "INSERT INTO dev.zkvmek963.genre_2024 (Genre) VALUES (%s)"
        for genre in repGenreNm:
            cur.execute(insert_query, (genre,))
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"ETL 과정 중 오류 발생: {error}")
        cur.execute("ROLLBACK;")
    finally:
        logging.info("ETL 작업 완료")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'Movie_Genre_Count',
    default_args=default_args,
    description='Genre Types of Movies Released in 2024',
    schedule ='0 1 * * *',
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    repGenreNm = fetch_movie_genre()
    etl_task = etl(repGenreNm)

    repGenreNm >> etl_task
