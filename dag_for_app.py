from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import pandas as pd
import pandahouse
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'a.figurova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 12),
}

schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_af():
    @task
    def feed_metrics():
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator'
        }
        q = """
            SELECT toDate(time) as day, user_id, IF(gender = 1, 'male', 'female') as gender,  multiIf(age <= 20, '0 - 20', age > 20
                      and age <= 30, '21-30', age > 30
                      and age <= 40, '31-40', age > 40
                      and age <= 50, '41-50', '50+') as age, os,
            countIf(user_id, action='like') as likes,
            countIf(user_id, action='view') as views
            FROM simulator_20220420.feed_actions
            WHERE toDate(time) = yesterday() 
            GROUP BY toDate(time), user_id, gender, age, os
            ORDER BY user_id
            """
        df_feed = pandahouse.read_clickhouse(query=q, connection=connection)
        return df_feed
    
    @task
    def message_metrics():
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator'
        }
        q = """
            SELECT day, user_id, SUM(messages_sent) as messages_sent, 
            SUM(users_sent) as users_sent, 
            SUM(messages_received) as messages_received,
            SUM(users_received) as users_received, IF(gender = 1, 'male', 'female')as gender, multiIf(age <= 20, '0 - 20', age > 20
                      and age <= 30, '21-30', age > 30
                      and age <= 40, '31-40', age > 40
                      and age <= 50, '41-50', '50+') as age, os
            FROM
            (SELECT toDate(time) as day, user_id,
            count(reciever_id) as messages_sent,
            count(distinct reciever_id) as users_sent, gender, age, os  
            FROM simulator_20220420.message_actions
            GROUP BY user_id, toDate(time), gender, age, os) sent
            JOIN
            (SELECT toDate(time) as day, reciever_id,
            count(user_id) as messages_received,
            count(distinct user_id) as users_received, gender, age, os  
            FROM simulator_20220420.message_actions
            GROUP BY reciever_id, toDate(time), gender, age, os) received ON sent.user_id = received.reciever_id 
            WHERE day = yesterday()
            GROUP BY user_id, day, gender, age, os
            ORDER BY user_id
            """
        df_message = pandahouse.read_clickhouse(query=q, connection=connection)
        return df_message
    
    @task
    def joining_tables(df_feed, df_message):
        df = pd.merge(df_feed, df_message, on=['user_id', 'day', 'gender', 'age', 'os'], how='outer')
        return df
        
    @task
    def transform_gender(df):
        metrics_gender = df.groupby(['gender', 'day']).sum().reset_index()
        metrics_gender['metrics'] = 'gender'
        metrics_gender.rename(columns={'day':'event_date'},inplace=True)
        metrics_gender.rename(columns={'gender':'metrics_value'},inplace=True)
        return metrics_gender
    
    @task
    def transform_age(df):
        metrics_age = df.groupby(['age', 'day']).sum().reset_index()
        metrics_age['metrics'] = 'age'
        metrics_age.rename(columns={'day':'event_date'},inplace=True)
        metrics_age.rename(columns={'age':'metrics_value'},inplace=True)
        return metrics_age
    
    @task
    def transform_os(df):
        metrics_os = df.groupby(['os', 'day']).sum().reset_index()
        metrics_os['metrics'] = 'os'
        metrics_os.rename(columns={'day':'event_date'},inplace=True)
        metrics_os.rename(columns={'os': 'metrics_value'}, inplace=True)
        return metrics_os
    
    @task
    def concat_table(metrics_gender, metrics_age, metrics_os):
        final_df = pd.concat([metrics_gender, metrics_age, metrics_os]).reset_index(drop=True)
        final_df  = final_df [['event_date','metrics','metrics_value', 'views', 'likes', 'messages_received', 'messages_sent','users_received', 'users_sent']]
        final_df ['views'] = final_df ['views'].astype(int)
        final_df ['likes'] = final_df ['likes'].astype(int)
        final_df ['messages_received'] = final_df ['messages_received'].astype(int)
        final_df ['messages_sent'] = final_df ['messages_sent'].astype(int)
        final_df ['users_received'] = final_df ['users_received'].astype(int)
        final_df ['users_sent'] = final_df ['users_sent'].astype(int)
        final_df ['event_date']=final_df ['event_date'].apply(lambda x: datetime.isoformat(x))

        return final_df 
    
    @task
    def result_table(final_df):
        connection_test = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': '656e2b0c9c',
        'user': 'student-rw',
        'database': 'test'
        }

        create = '''CREATE TABLE IF NOT EXISTS test.afigurova
            (
            event_date datetime,
            gender TEXT,
            age TEXT,
            os TEXT,
            views INTEGER,
            likes INTEGER,
            messages_received INTEGER,
            messages_sent INTEGER,
            users_received INTEGER,
            users_sent INTEGER
            ) ENGINE = MergeTree ORDER BY (event_date);
            '''
        pandahouse.execute(query=create, connection=connection_test)
        pandahouse.to_clickhouse(df=final_df, table='afigurova', index=False, connection=connection_test)
        
    df_feed = feed_metrics()
    df_message = message_metrics()
    df = joining_tables(df_feed, df_message)
    metrics_gender = transform_gender(df)
    metrics_age = transform_age(df)
    metrics_os = transform_os(df)
    final_df = concat_table(metrics_gender, metrics_age, metrics_os)
    result_table(final_df)
    
dag_af = dag_af()