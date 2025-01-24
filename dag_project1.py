import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры подключения
connection  = {'host': 'https://clickhouse.example',
                      'database':'database',
                      'user':'user', 
                      'password':'password'}


#дефолтные параметры
default_args = {
    'owner': 'e.bogomazova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024,11,20)}


#интервал запуска DAG
schedule_interval = '0 8 * * *' #ежедневно в 8:00


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_ebogomazova():

    @task 
    def extract_messages():
        query_messages = """
                    SELECT * 
                    FROM (
                        SELECT 
                        user_id, os, gender, age,
                        count(receiver_id) as messages_sent,
                        count(distinct(receiver_id)) as users_sent
                        FROM {db}.message_actions
                        WHERE toDate(time) = today()-1
                        GROUP BY user_id, os, gender, age) as messages_sent
                        
                    FULL OUTER JOIN (
                        SELECT receiver_id as user_id, os, gender, age,
                        count(user_id) as messages_received,
                        count(distinct(user_id)) as users_received
                        FROM {db}.message_actions
                        WHERE toDate(time)=today()-1
                        GROUP BY receiver_id, os, gender, age) as messages_received
                    using user_id, os, gender, age
                            """ 
        df_messages = ph.read_clickhouse(query_messages, connection=connection)
        return df_messages
    
    @task
    def extract_feed():
        query_feed = """
                    SELECT 
                          user_id, os, gender, age,
                          countIf(action = 'like') as likes,
                          countIf(action = 'view') as views
                    FROM {db}.feed_actions
                    WHERE toDate(time)=today()-1
                    GROUP By user_id, os, gender, age"""
         
        
        df_feed = ph.read_clickhouse(query_feed, connection=connection)
        return df_feed

    @task #объединение метрик
    def merge_df(df_feed, df_messages):
        combined_df = df_feed.merge(df_messages, on=['user_id', 'os', 'gender', 'age'], how='outer') \
            .fillna(0) \
            .astype({'views': 'int', 'likes': 'int',
                     'messages_received': 'int',
                     'messages_sent': 'int',
                     'users_received': 'int',
                     'users_sent': 'int'})
        return combined_df

    @task #срез пол
    def gender_metric(combined_df):
        df_gender_metric = combined_df[['gender', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
        .groupby(['gender'], as_index=False)\
        .sum()\
        .melt(['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])\
        .rename(columns={'variable':'dimension', 'value':'dimension_value'})
        return df_gender_metric
    
    @task #срез возраст
    def age_metric(combined_df):
        df_age_metric  = combined_df[['age', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
        .groupby('age', as_index=False)\
        .sum()\
        .melt(['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])\
        .rename(columns={'variable':'dimension', 'value':'dimension_value'})
        return df_age_metric
    
    @task #срез ОС
    def os_metric(combined_df):
        df_os_metric = combined_df[['os', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
        .groupby('os', as_index=False)\
        .sum()\
        .melt(['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])\
        .rename(columns={'variable':'dimension', 'value':'dimension_value'})
        return df_os_metric
    
    @task()
    def load(df_gender_metric, df_age_metric, df_os_metric):
        df_result = pd.concat([df_gender_metric, df_age_metric, df_os_metric])
        df_result['event_date'] = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        
        query_save = """
        create table if not exists test.ebogomazova_result
        (
        event_date Date,
        dimension String,
        dimension_value String,
        views Int64,
        likes Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64,
        users_sent Int64
        )
        engine = MergeTree()
        order by event_date
        """
        
        connection_write = {
            'host': 'https://clickhouse.example',
            'database': 'test',
            'user': 'user',
            'password': 'password'
        }
        
        ph.execute(query_save, connection=connection_write)
        ph.to_clickhouse(df_result, 'ebogomazova_result', 
                         connection=connection_write, index=False)
    


    # вызов тасков по порядку
    df_feed = extract_feed() 
    df_messages = extract_messages()
    
    combined_df = merge_df(df_feed, df_messages)
    
    df_gender_metric = gender_metric(combined_df)
    df_age_metric = age_metric(combined_df)
    df_os_metric = os_metric(combined_df)
        
    load(df_gender_metric, df_age_metric, df_os_metric)


dag_ebogomazova = dag_ebogomazova()

