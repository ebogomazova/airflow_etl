import telegram
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import io
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#коннект к бд
connection  = {'host': 'https://clickhouse.example',
                      'database':'database',
                      'user':'user', 
                      'password':'password'}

# Дефолтные параметры
default_args = {
    'owner': 'e.bogomazova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024,11,29), #начало запуска 
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *' #в 11:00

#для бота
chat_id = -*********
my_token = '**********:хххххххххххххххххххххххххххххх'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_ebogomazova_report():

    @task
    def extract_yesterday():
        q = '''
            SELECT
                toDate(time) AS dt,
                user_id,
                count(user_id) AS messages_sent,
                count(DISTINCT user_id) AS users_received,
                count(DISTINCT receiver_id) AS users_sent,
                0 AS likes,  
                0 AS views   
            FROM {db}.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id, dt

            UNION ALL

            SELECT
                toDate(time) AS dt,
                user_id,
                0 AS messages_sent, 
                0 AS users_received,
                0 AS users_sent,
                countIf(action = 'like') AS likes,
                countIf(action = 'view') AS views
            FROM {db}.feed_actions
            WHERE toDate(time) = yesterday()
            GROUP BY dt, user_id '''

        df = ph.read_clickhouse(q, connection = connection)
        return df
    
    @task
    def create_message1(df):
        
        dau_y = df['user_id'].nunique()
        likes_y = df['likes'].sum()
        views_y = df['views'].sum()
        ctr_y = likes_y / views_y
        messages_y = df['messages_sent'].sum()
        messages_r = df['users_received'].sum() / df['users_sent'].sum()


        msg = "Значение метрик за предыдущий день:\n"
        msg += "DAU Ленты и Мессенджера: {}\n".format(dau_y)
        msg += "Лайки: {}\n".format(likes_y)
        msg += "Просмотры: {}\n".format(views_y)
        msg += "CTR: {:.3f}\n".format(ctr_y)
        msg += "Отправлено сообщений: {}\n".format(messages_y)
        msg += "Получено/Отправлено: {:.3f}\n".format(messages_r)
        
        return msg

        
    @task
    def extract_week_m():
        q_m = """
               SELECT toDate(time) as dt,
               user_id, os, source,
               count(user_id) as messages_sent,
               count(distinct user_id) as users_received,
               count(distinct receiver_id) as users_sent
               FROM {db}.message_actions
               WHERE toDate(time) >= yesterday() - 7 and toDate(time) < yesterday()
               GROUP BY user_id, dt, os,source""" 

        df_m = ph.read_clickhouse(q_m, connection=connection)
        return df_m
    
    
    @task
    def extract_week_f():
        q_f = '''SELECT 
              user_id, os, source, toDate(time) as dt, 
              count(action) as ac,
              countIf(action = 'like') as likes,
              countIf(action = 'view') as views
              FROM {db}.feed_actions
              WHERE toDate(time) >= yesterday() - 7 and toDate(time) < yesterday()
              GROUP By dt, user_id, os, source'''

        df_f = ph.read_clickhouse(q_f, connection = connection)
        return df_f
    
    
    @task
    def create_plots(df_m, df_f):

        #метрики мессенджер
        dau_m = df_m.groupby('dt')['user_id'].nunique().reset_index(name = 'dau')  

        ms = df_m.groupby('dt')['messages_sent'].sum().reset_index(name = 'sent')
        mpu = (df_m.groupby('dt')['messages_sent'].sum() / df_m.groupby('dt')['user_id'].nunique()).reset_index(name='mpu')
        
        # метрики по feed
        dau_f = df_f.groupby('dt')['user_id'].nunique().reset_index(name = 'dau')  

        likes = df_f.groupby('dt')['likes'].sum().reset_index(name = 'likes')
        views = df_f.groupby('dt')['views'].sum().reset_index(name = 'views')
        
        ctr = (df_f.groupby('dt')['likes'].sum() / df_f.groupby('dt')['views'].sum()).reset_index(name='ctr')
        
        
        #Строим сетку 2 на 3 - 6 графиков с метриками на ней (dau, mau, wau) лента - мессенджер
        fig, axes = plt.subplots(2, 3, figsize=(25, 10))


        # График 1
        sns.lineplot(x='dt', y='dau', data=dau_f, ax=axes[0,0], color='blue')
        axes[0,0].set_title('DAU - Лента')
        axes[0,0].set_xlabel('')
        axes[0,0].set_ylabel('')
        axes[0,0].tick_params(axis='x', rotation=45)

        # График 2
        sns.lineplot(x='dt', y='views', data=views, ax=axes[0,1], color='blue', label='Просмотры')
        sns.lineplot(x='dt', y='likes', data=likes, ax=axes[0,1], color='green', label='Лайки')
        axes[0,1].set_title('Просмотры и Лайки - Лента')
        axes[0,1].set_xlabel('')
        axes[0,1].set_ylabel('')
        axes[0,1].tick_params(axis='x', rotation=45)
        axes[0,1].legend()

        # График 3
        sns.lineplot(x='dt', y='ctr', data=ctr, ax=axes[0,2], color='orange')
        axes[0,2].set_title('CTR - Лента')
        axes[0,2].set_xlabel('')
        axes[0,2].set_ylabel('')
        axes[0,2].tick_params(axis='x', rotation=45)

        # График 4
        sns.lineplot(x='dt', y='dau', data=dau_m, ax=axes[1,0], color='blue')
        axes[1,0].set_title('DAU - Мессенджер')
        axes[1,0].set_xlabel('')
        axes[1,0].set_ylabel('')
        axes[1,0].tick_params(axis='x', rotation=45)

        # График 5
        sns.lineplot(x='dt', y='sent', data=ms, ax=axes[1,1], color='purple')
        axes[1,1].set_title('Отправлено сообщений - Мессенджер')
        axes[1,1].set_xlabel('')
        axes[1,1].set_ylabel('')
        axes[1,1].tick_params(axis='x', rotation=45)

        # График 6
        sns.lineplot(x='dt', y='mpu', data=mpu, ax=axes[1,2], color='orange')
        axes[1,2].set_title('Сообщений на пользователя - Мессенджер')
        axes[1,2].set_xlabel('')
        axes[1,2].set_ylabel('')
        axes[1,2].tick_params(axis='x', rotation=45)


        plt.tight_layout()
        plt.show()

        plot_object = io.BytesIO() #создаем файловый объект
        plt.savefig(plot_object, format = 'png')
        plot_object.seek(0) #передвинули курсор в начало файлового объекта
        plot_name = 'weekly_plot.png'
        plt.close()
        return plot_object
    
    @task
    def create_message2():
        dashboard = 'https://superset.example/dashboard/6077/'
        msg2 = f"Подробнее в дашборде: {dashboard}"
        return msg2
    
    @task
    def send_report(msg, plot_object, msg2):
        bot = telegram.Bot(token = my_token)
        bot.sendMessage(chat_id = chat_id, text = msg)
        bot.sendPhoto(chat_id = chat_id, photo = plot_object)
        bot.sendMessage(chat_id = chat_id, text = msg2)

    #вызов тасков по порядку
    df = extract_yesterday()
    msg = create_message1(df)
    df_m = extract_week_m()
    df_f = extract_week_f()
    plot_object = create_plots(df_m, df_f)
    msg2 = create_message2()
    send_report(msg, plot_object, msg2)
   
        
dag_ebogomazova_report = dag_ebogomazova_report()