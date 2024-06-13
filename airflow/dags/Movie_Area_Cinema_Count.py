from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests
import logging
import psycopg2
import pandas as pd


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def fetch_movie_seat_cnt():
    res = requests.get("https://www.kobis.or.kr/kobis/business/mast/thea/findAreaTheaterStat.do")
    soup = BeautifulSoup(res.text, 'html.parser')
    
    all_data = soup.find_all("td")
    
    clean_data = [data.get_text(strip=True) for data in all_data]
    
    num_columns = 3
    regions = clean_data[::num_columns + 6]  # 각 지역명 추출
    values = [clean_data[i:i+num_columns] for i in range(1, len(clean_data), num_columns + 6)]  # 각 지역별 값 추출
    
    df = pd.DataFrame(values, columns=['CINEMA_CNT', 'SCREEN_CNT', 'SEAT_CNT'])
    df.insert(0, 'DEPT_NAME, ', regions)
    
    df = df[:17] # 합계 컬럼 제거
    df['CINEMA_CNT'] = pd.to_numeric(df['CINEMA_CNT'], errors='coerce')
    df['SCREEN_CNT'] = pd.to_numeric(df['SCREEN_CNT'], errors='coerce')
    df['SEAT_CNT'] = df['SEAT_CNT'].str.replace(',', '').astype(int)
    return df
    

@task
def etl(df):
    cur = get_Redshift_connection()
    
    try:
        logging.info("ETL 시작")
        cur.execute("BEGIN;")
        
        cur.execute("""
            DROP TABLE IF EXISTS dev.zkvmek963.area_cinema;
        """)
        cur.execute("""
            CREATE TABLE dev.zkvmek963.area_cinema (
            DEPT_NAME VARCHAR(255),
            CINEMA_CNT INT,
            SCREEN_CNT INT,
            SEAT_CNT INT
        )
        """)
        insert_query = "INSERT INTO dev.zkvmek963.area_cinema (DEPT_NAME, CINEMA_CNT, SCREEN_CNT, SEAT_CNT) VALUES (%s, %s, %s, %s)"

        for data in df.values.tolist(): # df.values.tolist() >> df를 2차원 리스트로 변환
            cur.execute(insert_query, (data[0], data[1], data[2],data[3]))

        # 변경 사항 커밋
        cur.execute("COMMIT;")
        
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
    'Movie_Area_Cinema_Count',
    default_args=default_args,
    description='Number of movie theaters by region',
    schedule ='0 1 * * 1',
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    seat_cnt = fetch_movie_seat_cnt()
    etl_task = etl(seat_cnt)

    seat_cnt >> etl_task
