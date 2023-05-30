import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import time
import psycopg2
import pandas as pd
import configparser
from psycopg2 import sql
config= configparser.ConfigParser()
config.read_file(open("/home/sudip/airflow/dags/credentials.cfg"))
host=config.get('REDSHIFT','HOST')
port = config.get('REDSHIFT','PORT')
database = config.get('REDSHIFT','DATABASE')
user = config.get('REDSHIFT','USER')
password = config.get('REDSHIFT','PASSWORD')
nepse_today_scrapper = "/home/sudip/airflow/dags/stockmarket/"
home= "/home/sudip/airflow"

from airflow.utils import timezone
from datetime import timedelta

dag = DAG(
    dag_id="nepseDag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/60 11-17 * * 1-4,6-7",

    # schedule_interval='*/3 * * * *',

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
        df= pd.read_csv(f"{home}/data/nepsetoday.csv")
        #rmove the row with company_nam is null
        df=df[df["company_name"].notna()]
        df=df[df["company_name"]!="company_name"]
        columns_list =['confidence', 'open_price', 'lowest_price',
        'highest_price', 'closing_price', 'VWAP', 'total_traded_quantity',
        'Previous_closing', 'total_traded_value', 'total_trades', 'difference',
        'range', 'difference_percentage', 'range_percentage', 'VWAP_percentage',
        'year_high', 'year_low']
        try:
            for column in columns_list:
                df[column] = df[column].apply(lambda x: float(x.replace(",","")) if type(x)==str else x)
        except:
            pass
        df.to_csv(f"{home}/data/nepsetoday.csv",index=False)       
    except:
        print("nepsetoday.csv not found") 
def csvMerger():
    try:
        if os.path.exists(f"{home}/data/nepsetoday.csv") :
            #merge data.csv and data1.csv and arrange according to date in descending order
            nepseToday=pd.read_csv(f"{home}/data/nepsetoday.csv")
            nepseExceptToday=pd.read_csv(f"{home}/data/nepsedata.csv")
            # get the date of the last row of nepseToday
            lastDate=nepseToday.iloc[-1]["date"]
            #now delete the nepseExceptToday rows date equal to lastDate
            nepseExceptToday=nepseExceptToday[nepseExceptToday["date"]!=lastDate]
            #now merge the two dataframes in descending order of date
            df=pd.concat([nepseToday,nepseExceptToday],axis=0)
            df=df.sort_values(by="date",ascending=False)
            df.to_csv(f"{home}/data/nepsedata.csv",index=False)
            time.sleep(5)
            #now delete the nepse_today.csv file
        else:
            print("nepsetoday.csv not found")
            pass
            
    except:
        print("nepsetoday.csv not found")
        pass
        
def updatewarehouse():
    print("updating warehouse")
    print(host)
    #connect to the database
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    cursor = conn.cursor()
    print("connected to the database",conn)
    
    #now read the csv file
    
    nepseToday = pd.read_csv(f"{home}/data/nepsetoday.csv")
    lastDate=nepseToday.iloc[-1]["date"]
    ##now search for the lastDate in the warehouse
    cursor.execute(f"select * from nepse where date='{lastDate}'")
    print("searching for lastDate in warehouse")
    rows=cursor.fetchall()
    if len(rows)!=0:
        #delete all the rows with date equal to lastDate
        cursor.execute(f"delete from nepse where date='{lastDate}'")
        conn.commit()

    #now insert the rows of nepseToday to warehouse
    data = [tuple(row) for row in nepseToday.values]
    sql = 'INSERT INTO nepse VALUES %s'
    args_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in data)
    cursor.execute(sql % args_str)
    conn.commit()
    print("warehouse updated")
    os.remove(f"{home}/data/nepsetoday.csv")
   
                
        
    
    
    

t1 = BashOperator(
    task_id="scrape",
    bash_command=f"cd /home/sudip/airflow/stockmarket/ && scrapy crawl nepse-today",
    dag=dag
)
t2= PythonOperator(
    task_id="nepsetodayCleaner",
    python_callable=nepse_today_cleaner,
    dag=dag
)

t3 = PythonOperator(
    task_id="csvMerger",
    python_callable=csvMerger,
    dag=dag
)
t4 = PythonOperator(
    task_id="updatewarehouse",
    python_callable=updatewarehouse,
    dag=dag
)

##run t1 and t2 sequentially and t3 and t4 in parallel
t1 >> t2 >> [t3,t4]
