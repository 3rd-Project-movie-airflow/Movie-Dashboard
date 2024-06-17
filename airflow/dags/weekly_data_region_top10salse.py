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
    weekday = today.weekday() + 1  # 월요일은 0, 일요일은 6
    last_sunday = today - timedelta(days=weekday)
    return last_sunday.strftime('%Y%m%d')

@task
def ET_get_area_code(last_sunday, schema):
    cur = get_Redshift_connection()
    get_regions_sql = f"SELECT * FROM {schema}.area_codes;"
    cur.execute(get_regions_sql)
    regions = cur.fetchall()
    sales_for_regions = {}
    for row in regions:
        area_code = row[1]
        region_name = row[2]
        url = Variable.get("api_key") + f"&targetDt={last_sunday}&weekGb=0&wideAreaCd=0{area_code}"
        response = requests.get(url)
        data = response.json()
        logging.info(f"Extracting data for date: {last_sunday} {region_name}")
        lines = data["boxOfficeResult"]["weeklyBoxOfficeList"]
        sales = sum(int(line["salesAmt"]) for line in lines)
        sales_for_regions[region_name] = sales
        logging.info(f"Transform ended for {region_name}")
    return sales_for_regions

@task
def Load(schema, table, records):
    logging.info("Load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                region_name VARCHAR(50),
                sales_thisweek BIGINT
            );
        """)
        cur.execute(f"DELETE FROM {schema}.{table};")
        insert_sql = f"""
            INSERT INTO {schema}.{table} (region_name, sales_thisweek)
            VALUES (%s, %s);
        """
        for key, value in records.items():
            cur.execute(insert_sql, (key, value))
        cur.execute("COMMIT;")  # 트랜잭션 커밋
    except Exception as error:
        logging.error(error)
        cur.execute("ROLLBACK;")  # 트랜잭션 롤백
    logging.info("Load done")

@task
def join_tables(schema, output_table):
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        drop_table_query = f"DROP TABLE IF EXISTS {schema}.{output_table};"
        logging.info(f"Executing query: {drop_table_query}")
        cur.execute(drop_table_query)
        join_query = f"""
        CREATE TABLE {schema}.{output_table} AS
        SELECT wa.region_name, wa.sales_thisweek, md.dept_id
        FROM {schema}.weekly_sales_top10SUM wa
        JOIN {schema}.area_codes md
        ON wa.region_name = md.dept_name;
        """
        logging.info(f"Executing query: {join_query}")
        cur.execute(join_query)
        cur.execute("COMMIT;")  # 트랜잭션 커밋
        logging.info(f"Joined data inserted into {schema}.{output_table} successfully")
    except Exception as error:
        logging.error(error)
        cur.execute("ROLLBACK;")  # 트랜잭션 롤백
        raise

with DAG(
    'weekly_region_sales_top10SUM',
    start_date=datetime(2024, 6, 8),
    schedule_interval='10 0 * * 1',  # 매주 월요일 00:10
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    schema = 'zkvmek963'
    table = 'weekly_sales_top10SUM'
    output_table = 'joined_table_sales_audience_top10SUM'

    last_sunday = get_last_sunday()
    extracted_transformed_data = ET_get_area_code(last_sunday, schema)
    load_task = Load(schema, table, extracted_transformed_data)
    join_task = join_tables(schema, output_table)

    last_sunday >> extracted_transformed_data >> load_task >> join_task