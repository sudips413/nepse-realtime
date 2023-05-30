import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import time
nepse_today_scrapper = "/home/sudip/airflow/dags/stockmarket/"
home= "/home/sudip/airflow"

from airflow.utils import timezone
from datetime import timedelta

dag = DAG(
    dag_id="nepseDagRealtime",
    start_date=datetime(2023, 1, 1),
    # ##evrey 2 mins schedle 
    schedule_interval="*/2 * * * *",
    # schedule_interval="*/2 11-17 * * 1-5,6-7",  # Run every 5 minutes between 11:30 AM and 03:00 PM every day.
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1),
        "timezone": "Asia/Kathmandu",
    },
)


def nepse_today_cleaner():
    print("cleaning")
    try:
        df= pd.read_csv(f"{home}/data/nepsetodayRealtime.csv")
        print(df.head(5))
        print("into cleaning")
        df=df[df["company_name"].notna()]
        df=df[df["company_name"]!="company_name"]
        columns_list =["open_price","highest_price","lowest_price","total_traded_quantity",
                            "total_traded_value",
                            "total_trades",
                            "LTP",
                            "previous_closing",
                            "average_traded_price",
                            "year_high",
                            "year_low",]
        try:
            for column in columns_list:
                df[column] = df[column].apply(lambda x: float(x.replace(",","")) if type(x)==str else x)
        except:
            print("float conversion error")
        df.to_csv(f"{home}/data/nepsetodayRealtime.csv",index=False)       
    except:
        print("nepsetodayRealtime.csv not found")


t1 = BashOperator(
    task_id="scrape",
    bash_command=f"cd /home/sudip/airflow/stockmarket/ && scrapy crawl nepse-today-realtime ",
    dag=dag
)
t2= PythonOperator(
    task_id="nepsetodayCleaner",
    python_callable=nepse_today_cleaner,
    dag=dag
)

t1 >> t2
