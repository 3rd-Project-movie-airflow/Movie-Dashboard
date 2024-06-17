from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_last_sunday():
    today = datetime.today()
    weekday = today.weekday() + 1  # 오늘의 요일 (월요일은 0, 일요일은 6)
    last_sunday = today - timedelta(days=weekday)  # 지난주 일요일 구하기
    return last_sunday.strftime('%Y%m%d')

@task
def ET_get_area_code(last_sunday,schema):
    cur = get_Redshift_connection()
    get_regions_sql = f"SELECT * FROM {schema}.area_codes;"
    cur.execute(get_regions_sql)
    regions = cur.fetchall()
    audi_for_regions = {}
    for row in regions:
        area_code = row[1]
        region_name = row[2]
        url = Variable.get("api_key") + f"&targetDt={last_sunday}" + f"&weekGb=0&wideAreaCd=0{area_code}"
        logging.info(f"Fetching data for {region_name} with area code {area_code}")
        response = requests.get(url)
        data = response.json()
        lines = data["boxOfficeResult"]["weeklyBoxOfficeList"]
        audi_list = [int(line["audiCnt"]) for line in lines]
        audi_for_regions[region_name] = audi_list
        logging.info(f"Data fetched and transformed for {region_name}")
    return audi_for_regions

@task
def Load(schema, table, records):
    logging.info("Load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                region_name VARCHAR(50),
                rank1 BIGINT, 
                rank2 BIGINT, 
                rank3 BIGINT, 
                rank4 BIGINT, 
                rank5 BIGINT,
                rank6 BIGINT, 
                rank7 BIGINT, 
                rank8 BIGINT, 
                rank9 BIGINT, 
                rank10 BIGINT
            );
        """)
        for key, value in records.items():
            while len(value) < 10:  # Ensure there are exactly 10 ranks
                value.append(0)
            sql = f"""
            INSERT INTO {schema}.{table} (region_name, rank1, rank2, rank3, rank4, rank5, rank6, rank7, rank8, rank9, rank10)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cur.execute(sql, [key] + value)
        cur.execute("COMMIT;")  # 트랜잭션 커밋
    except Exception as error:
        logging.error(error)
        cur.execute("ROLLBACK;")  # 트랜잭션 롤백
    logging.info("Load done")

with DAG(
    'weekly_region_audience_top10',
    start_date=datetime(2024, 6, 8),
    schedule ='10 0 * * 1',  # 매주 월요일 00:10
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    schema = 'dev.zkvmek963'
    table = 'weekly_audience_top10'
    
    last_sunday = get_last_sunday()
    extracted_transformed_data = ET_get_area_code(last_sunday,schema)
    Load(schema, table, extracted_transformed_data)

    last_sunday >> extracted_transformed_data >> Load(schema, table, extracted_transformed_data)