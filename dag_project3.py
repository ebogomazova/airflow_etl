import telegram
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import pandahouse as ph
import io
import pandas as pd
from io import StringIO
import requests
from datetime import datetime, timedelta
import sys
import os
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context

#параметры подключения к бд
connection  = {'host': 'https://clickhouse.example',
                      'database':'database',
                      'user':'user', 
                      'password':'password'}

# Дефолтные параметры
default_args = {
    'owner': 'e.bogomazova',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024,12,10), #начало
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *' #каждые 15 минут


#для бота
chat_id = -*********
my_token = '**********:хххххххххххххххххххххххххххххх'
bot = telegram.Bot(token = my_token)

#ссылка на дашборд
dashboard_url = 'https://superset.example/superset/dashboard/6076/'

#ключевые метрики
metrics_list = ['users_feed', 'likes', 'views', 'CTR', 'users_message', 'messages']


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def ebogomazova_aa():
    
    @task
    def extract_data():
        q = """
            SELECT
                    toStartOfFifteenMinutes(fa.time) AS ts,
                    toDate(fa.time) AS date,
                    formatDateTime(toStartOfFifteenMinutes(fa.time), '%R') AS hm,
                    count(distinct fa.user_id) AS users_feed,
                    countIf(fa.action = 'like') AS likes,
                    countIf(fa.action = 'view') AS views,
                    countIf(fa.action = 'like') / countIf(fa.action = 'view') AS CTR,
                    count(distinct ma.user_id) AS users_message,
                    count(ma.user_id) AS messages
                FROM
                    {db}.feed_actions fa
                    LEFT JOIN {db}.message_actions ma ON fa.time = ma.time AND fa.user_id = ma.user_id
                WHERE
                    fa.time >= today() - 1 AND fa.time < toStartOfFifteenMinutes(now())
                GROUP BY
                    ts, date, hm
                ORDER BY
                    ts"""

        df = ph.read_clickhouse(q, connection=connection)
        df['ts'] = pd.to_datetime(df['ts'])
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    
    @task
    def check_anomaly(df):
        n = 5 #ширина интервала
        a = 3 #кол-во периодов
        df = df.copy()
        for metric in metrics_list:
            df[f'q25_{metric}'] = df[metric].shift(1).rolling(n, min_periods=1).quantile(0.25).fillna(method='bfill')
            df[f'q75_{metric}'] = df[metric].shift(1).rolling(n, min_periods=1).quantile(0.75).fillna(method='bfill')
            df[f'iqr_{metric}'] = df[f'q75_{metric}'] - df[f'q25_{metric}']
            df[f'upper_{metric}'] = df[f'q75_{metric}'] + a * df[f'iqr_{metric}']
            df[f'lower_{metric}'] = df[f'q25_{metric}'] - a * df[f'iqr_{metric}']

            df[f'anomaly_{metric}'] = np.where((df[metric] > df[f'upper_{metric}']) | (df[metric] < df[f'lower_{metric}']), 1, 0)

            df[f'deviation_{metric}'] = 0.0  # инициализация столбца

            # Фильтрация строк, где anomaly_metric == 1
            anomaly_rows = df[df[f'anomaly_{metric}'] == 1].index

            df.loc[anomaly_rows, f'deviation_{metric}'] = np.where(
                 df.loc[anomaly_rows, f'q75_{metric}'] != 0,
                ((df.loc[anomaly_rows, metric] - df.loc[anomaly_rows, f'q75_{metric}']) / df.loc[anomaly_rows, f'q75_{metric}'])*100,
                ((df.loc[anomaly_rows, f'q25_{metric}'] - df.loc[anomaly_rows, metric]) / df.loc[anomaly_rows, f'q25_{metric}'])*100
            )

            df[f'deviation_{metric}'] = df[f'deviation_{metric}'].replace([np.inf, -np.inf], 0)
        return df
    
    @task
    def create_message(df):
        last_15_minutes = df.iloc[-1]
        message = ""
        anomaly_detected = False  # Флаг для проверки наличия аномалий
        for metric in metrics_list:
            if last_15_minutes[f'anomaly_{metric}'] == 1:
                metric_value = last_15_minutes[metric]
                metric_time = last_15_minutes['hm']
                deviation = last_15_minutes[f'deviation_{metric}']
                message += f"Метрика {metric}: Аномалия обнаружена в {metric_time}. Текущее значение: {metric_value:.2f}. Отклонение: {deviation:.2f}%\n"
                anomaly_detected = True  #Установка флага

        if anomaly_detected:  #проверка флага
            message += f" Подробнее в {dashboard_url}"
            return message
        else:
            return None  #возвращаем None, если нет аномалий
    
    @task
    def create_plot(df):
        last_15_minutes = df.iloc[-1]
        df_plot = df.tail(10)
        anomaly_metrics = [metric for metric in metrics_list if last_15_minutes[f'anomaly_{metric}'] == 1]
        if not anomaly_metrics:
            return None

        num_plots = len(anomaly_metrics)
        num_cols = min(num_plots, 2)
        num_rows = (num_plots + num_cols - 1) // num_cols

        fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 5 * num_rows))

        #Обработка случая одного графика
        if num_plots == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        for i, metric in enumerate(anomaly_metrics):
            ax = axes[i]
            ax.plot(df_plot['ts'], df_plot[metric], label=metric)
            ax.plot(df_plot['ts'], df_plot[f'upper_{metric}'], label=f'upper_{metric}', linestyle=':')
            ax.plot(df_plot['ts'], df_plot[f'lower_{metric}'], label=f'lower_{metric}', linestyle=':')


            ax.set_xlabel('Время')
            ax.set_ylabel(metric)
            ax.legend()
            ax.set_title(f'График {metric}')
            ax.tick_params(axis='x', rotation=45)

        for j in range(i + 1, len(axes)):
            fig.delaxes(axes[j])

        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png')
        plot_object.seek(0)
        plt.close()
        return plot_object

    @task
    def send_alert(message, plot_object):
        try:
            if message: # Проверка на None
                bot.send_message(chat_id=chat_id, text=message)
                if plot_object:
                    bot.send_photo(chat_id=chat_id, photo=plot_object)
        except Exception as e:
            print(f"Ошибка при отправке сообщения или фотографии: {e}")


    #вызов тасков по порядку
    df = extract_data()
    df = check_anomaly(df)
    message = create_message(df)
    plot_object = create_plot(df)
    send_alert(message, plot_object)

ebogomazova_aa = ebogomazova_aa()