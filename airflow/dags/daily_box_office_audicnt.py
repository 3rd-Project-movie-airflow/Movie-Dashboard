from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from datetime import datetime, timedelta
import requests
import psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    api_key = Variable.get('d_boxoffice_api_key')
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)
    
    records = []
    for single_date in (start_date + timedelta(n) for n in range(7)):
        targetDt = single_date.strftime("%Y%m%d")
        url = f"http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={api_key}&targetDt={targetDt}"

        response = requests.get(url).json()
        box_office_list = response['boxOfficeResult']['dailyBoxOfficeList']

        for list in box_office_list:
            record = (
                list['movieNm'],
                single_date.strftime("%Y-%m-%d"),
                list['rank'],
                list['audiCnt']
            )
            records.append(record)

    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        sql = f'''
        DROP TABLE IF EXISTS {schema}.{table};
        CREATE TABLE {schema}.{table}(
            movie_title VARCHAR(256),
            target_date DATE,
            rank INT,
            audi_cnt INT,
            PRIMARY KEY (movie_title, target_date)
        ); '''
        cur.execute(sql)

        # 새 데이터를 삽입
        insert_query = f"""
        INSERT INTO {schema}.{table} (movie_title, target_date, rank, audi_cnt)
        VALUES (%s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")   
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise 

dag = DAG(
    dag_id='daily_box_office_audicnt',
    start_date=datetime(2024, 6, 1),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl,
    params={
        'schema': 'zkvmek963',
        'table': 'daily_box_office_audicnt'
    },
    dag=dag
)
