from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl():
    cur = get_Redshift_connection()
    url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
    params = {
        'key': Variable.get("movie_api_key_3"),
        'targetDt': (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    }

    res = requests.get(url, params=params)
    data = res.json()['boxOfficeResult']['dailyBoxOfficeList']
    df = pd.DataFrame(data)
    df = df[['movieCd', 'movieNm', 'audiCnt']]
    df['audiCnt'] = df['audiCnt'].astype(int)

    for index, a in df.iterrows():
        movieCd = a['movieCd']
        movieNm = a['movieNm']
        audiCnt = a['audiCnt']

        cur.execute(f"""
            SELECT *
            FROM zkvmek963.boxoffice_audicnt
            WHERE movieCd = '{movieCd}'
        """)
        
        if cur.fetchone() is None:
            cur.execute(f"""
                INSERT INTO zkvmek963.boxoffice_audicnt VALUES ('{movieCd}', '{movieNm}', {audiCnt})
            """)
        else:
            cur.execute(f"""
                UPDATE zkvmek963.boxoffice_audicnt
                SET audi_cnt = audi_cnt + {audiCnt}
                WHERE movieCd = '{movieCd}'
            """)

    cur.close()

dag = DAG(
    'update_boxoffice_audiCnt',
    description='Update box office_audiCnt data in Redshift',
    start_date=datetime(2024, 6, 10, 12, 0),
    schedule_interval='0 12 * * *',
    catchup=False
)

boxoffice_audiCnt_task = PythonOperator(
    task_id='boxoffice_audiCnt_task',
    python_callable=etl,
    provide_context=True,
    dag=dag,
)

boxoffice_audiCnt_task