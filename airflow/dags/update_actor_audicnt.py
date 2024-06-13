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
    data = res.json()
    data = data['boxOfficeResult']['dailyBoxOfficeList']

    df = pd.DataFrame(data)
    df = df[['movieCd', 'movieNm', 'audiCnt']]
    df['audiCnt'] = df['audiCnt'].astype(int)

    for index, a in df.iterrows():
        movieCd = a['movieCd']
        audiCnt = a['audiCnt']

        url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json'
        params = {
            'key': Variable.get("movie_api_key_3"),
            'movieCd': movieCd
        }

        res = requests.get(url, params=params)
        data = res.json()
        data = data['movieInfoResult']['movieInfo']['actors']

        actor_df = pd.DataFrame(data)

        if 'peopleNm' not in actor_df.columns:
            continue

        temp = actor_df[['peopleNm']].head(15)

        for _, act_list in temp.iterrows():
            peopleNm = act_list['peopleNm']
            cur.execute(f"""
                SELECT peopleNm
                FROM zkvmek963.actor_audicnt
                WHERE peopleNm = '{peopleNm}'
            """)

            if cur.fetchone() is None:
                cur.execute(f"""
                    INSERT INTO zkvmek963.actor_audicnt VALUES ('{peopleNm}', {audiCnt})
                """)
            else:
                cur.execute(f"""
                    UPDATE zkvmek963.actor_audicnt
                    SET audi_cnt = audi_cnt + {audiCnt}
                    WHERE peopleNm = '{peopleNm}'
                """)

    cur.close()

dag = DAG(
    'update_actor_audiCnt',
    description='Update actor sales and audience count data in Redshift',
    start_date=datetime(2024, 6, 10, 12, 0),
    schedule_interval='0 12 * * *',
    catchup=False
)

update_actor_audiCnt_task = PythonOperator(
    task_id='update_actor_audiCnt_task',
    python_callable=etl,
    dag=dag,
)

update_actor_audiCnt_task