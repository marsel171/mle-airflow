# скопируйте код ниже #

# dags/clean_churn.py

import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():
        from sqlalchemy import Table, Column, DateTime, Float, Integer, Index, MetaData, String, UniqueConstraint, inspect
        hook = PostgresHook(# Ваш код здесь #)
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        churn_table = Table(# Ваш код здесь #, metadata,
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
            UniqueConstraint('customer_id', name='unique_clean_customer_constraint')
        )
        if not inspect(db_engine).has_table(churn_table.name):
            metadata.create_all(db_engine)
    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = # Ваш код здесь #
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):

        

        def remove_duplicates(data):
            feature_cols = data.columns.drop('customer_id').tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data



        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
        hook.insert_rows(
            table= # Ваш код здесь #,
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_churn_dataset()