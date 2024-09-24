from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, UniqueConstraint, inspect, Float

def create_table() -> None:
    db_conn = PostgresHook("destination_db").get_sqlalchemy_engine().connect()
    metadata = MetaData()
    table = Table('alt_users_churn', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String),
        Column('begin_date', DateTime),
        Column('end_date', DateTime),
        Column('type', String),
        Column('paperless_billing', String),
        Column('payment_method', String),
        Column('monthly_charges', Float),
        Column('total_charges', Float),
        Column('internet_service', String),
        Column('online_security', String),
        Column('online_backup', String),
        Column('device_protection', String),
        Column('tech_support', String),
        Column('streaming_tv', String),
        Column('streaming_movies', String),
        Column('gender', String),
        Column('senior_citizen', Integer),
        Column('partner', String),
        Column('dependents', String),
        Column('multiple_lines', String),
        Column('target', Integer),
        UniqueConstraint('customer_id', name='unique_customer_id_constraint_alt_churn')
        )
    if not inspect(db_conn).has_table(table.name): 
        metadata.create_all(db_conn) 

def extract(**kwargs)-> None:
    conn = PostgresHook('source_db').get_conn()
    sql = f"""
    select
        c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
        i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
        p.gender, p.senior_citizen, p.partner, p.dependents,
        ph.multiple_lines
    from contracts as c
    left join internet as i on i.customer_id = c.customer_id
    left join personal as p on p.customer_id = c.customer_id
    left join phone as ph on ph.customer_id = c.customer_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    ti = kwargs['ti']
    ti.xcom_push('extracted_data', data)
    
def transform(**kwargs)-> None:
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='extract', key='extracted_data') # выгрузка данных из task_instance
    data['target'] = (data['end_date'] != 'No').astype(int) # логика функции
    data['end_date'].replace({'No': None}, inplace=True)
    ti.xcom_push('transformed_data', data) # вместо return отправляем данные передатчику task_instance 

def load(**kwargs)-> None:
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='transform', key='transformed_data') # выгрузка данных из task_instance
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
        ) 