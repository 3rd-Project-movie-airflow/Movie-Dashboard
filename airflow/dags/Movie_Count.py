from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests
import logging
import psycopg2  # 예외 처리를 위해 추가

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def fetch_movie_count(start: int, end: int):
    api_key = Variable.get("movie_api_key")
    movie_counts = []

    for i in range(start, end + 1):
        res = requests.get(f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={api_key}&openStartDt={i}&openEndDt={i}")
        data = res.json()
        movie_counts.append([i, data['movieListResult']['totCnt']])
    return movie_counts

@task
def etl_cnt(movie_counts):
    cur = get_Redshift_connection()
    
    try:
        cur.execute("BEGIN;")
        
        # 테이블 삭제 후 생성 쿼리 실행
        cur.execute("""
            DROP TABLE IF EXISTS dev.zkvmek963.movie_cnt;
        """)
        cur.execute("""
            CREATE TABLE dev.zkvmek963.movie_cnt (
                YEAR INT,
                COUNT INT
            )
        """)

        insert_query = "INSERT INTO dev.zkvmek963.movie_cnt (YEAR, COUNT) VALUES (%s, %s)"

        for m in movie_counts:
            cur.execute(insert_query, (m[0], m[1]))

        # 변경 사항 커밋
        cur.execute("COMMIT;")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
    finally:
        print("Process Complete")
        logging.info("load done")

# DAG 선언
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Movie_Count',
    default_args=default_args,
    description='A simple DAG to fetch and store movie counts',
    schedule ='0 1 * * 1',
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    start_year = 2012
    end_year = 2024

    movie_counts = fetch_movie_count(start_year, end_year)
    etl_task = etl_cnt(movie_counts)

    movie_counts >> etl_task
