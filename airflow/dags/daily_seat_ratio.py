from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract_transform():
    current_date = datetime.now()
    url = "https://www.kobis.or.kr/kobis/business/stat/boxs/findDailySeatTicketList.do"
    data = {
        "loadEnd": "0"
    }

    response = requests.post(url, data=data)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        records = []
        record_date = current_date
        for tbody in soup.find_all("tbody", id=lambda x: x and x.startswith('tbody_')):
            record_date = record_date - timedelta(days=1)
            for tr in tbody.find_all("tr"):
                seat_cnt = int(tr.find("td", id="td_totSeatCnt").text.strip().replace(',', ''))
                movie_title = tr.find("td", id="td_movie").span.text.strip()
                seat_ratio = float(tr.find("td", id="td_totSeatPerRatio").text.strip().replace('%', ''))
                target_date = record_date.strftime("%Y-%m-%d")
                
                records.append((movie_title, target_date, seat_ratio, seat_cnt))
        return records

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="extract_transform")    
    
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        sql = f'''
        DROP TABLE IF EXISTS {schema}.{table};
        CREATE TABLE {schema}.{table}(
            movie_title VARCHAR(256),
            target_date DATE,
            seat_ratio NUMERIC(4, 1),
            seat_cnt INT,
            PRIMARY KEY (movie_title, target_date)
        ); '''
        cur.execute(sql) 
        insert_query = f"""
        INSERT INTO {schema}.{table} (movie_title, target_date, seat_ratio, seat_cnt)
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
    dag_id='daily_seat_ratio',
    start_date=datetime(2024, 6, 1),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract_transform_task = PythonOperator(
    task_id='extract_transform',
    python_callable=extract_transform,
    params={
    },
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'zkvmek963',
        'table': 'daily_seat_ratio'
    },
    dag=dag
)

extract_transform_task >> load_task
