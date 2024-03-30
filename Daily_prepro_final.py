import re
import time
import logging
import pymysql
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
from airflow import DAG
from sqlalchemy import text
from bs4 import BeautifulSoup
from airflow.decorators import dag
from airflow.models.dag import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from sqlalchemy import create_engine, VARCHAR, DATE
from airflow.operators.python_operator import PythonOperator
from urllib.parse import quote
from requests.exceptions import RequestException
pymysql.install_as_MySQLdb()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Daily_prepro_final',  
    default_args=default_args,  
    description='Daily_prepro_final',  
    start_date=datetime(2024, 3, 8),
    schedule_interval='0 3 * * *',
    tags=['preprocess_location'],
    catchup=False
)

def con_database_news_table_prepro2():
    connection = pymysql.connect(
        host='host ip',
        user='user name',
        password='db pw',
        database='db name',
        port= port num
    )
    connection_url = f"mysql+pymysql://user name:db pw@host ip:port num/db name"
    engine = create_engine(connection_url)
    query = "SELECT * FROM News_table_prepro3 WHERE news_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"
    final_df = pd.read_sql_query(query, connection)
    if final_df.empty:  
            return None
    else:
        return final_df

# p_location 값으로 좌표 추출
def location(**kwargs):
    final_df = kwargs['ti'].xcom_pull(task_ids='con_database_news_table_prepro2_task')
    if final_df is not None:
        for index, row in final_df.iterrows():
            keyword = row['p_location']

            # 부동 소수점인지 문자열인지 확인하고 처리
            if isinstance(keyword, str):
                encoded_keyword = quote(keyword.encode('utf-8'))
                url = f"api url={encoded_keyword}api url"
                headers = {
                    "Accept": "application/json",
                    "appKey": "appkey"
                }
                response = requests.get(url, headers=headers)
                try:
                    r = response.json()
                    final_df.loc[index, 'frontLat'] = r['searchPoiInfo']['pois']['poi'][0]['frontLat']
                    final_df.loc[index, 'frontLon'] = r['searchPoiInfo']['pois']['poi'][0]['frontLon']
                    final_df.loc[index, 'upperAddrName'] = r['searchPoiInfo']['pois']['poi'][0]['upperAddrName']
                    final_df.loc[index, 'middleAddrName'] = r['searchPoiInfo']['pois']['poi'][0]['middleAddrName']
                except:
                    final_df.loc[index, 'frontLat'] = None
                    final_df.loc[index, 'frontLon'] = None
                    final_df.loc[index, 'upperAddrName'] = None
                    final_df.loc[index, 'middleAddrName'] = None
                    
            else:
                final_df.loc[index, 'frontLat'] = None
                final_df.loc[index, 'frontLon'] = None
                final_df.loc[index, 'upperAddrName'] = None
                final_df.loc[index, 'middleAddrName'] = None
    if final_df is not None:
        print("Length of final_df:", len(final_df))
    return final_df

# null 값 처리
def null_prepro(**kwargs):
    final_df = kwargs['ti'].xcom_pull(task_ids='location_task')
    if final_df is not None:
        # 'None' 값을 NaN으로 변환
        final_df.replace('None', np.nan, inplace=True)
        # frontLat 및 frontLon 열 값이 None인 행 삭제
        final_df = final_df.dropna(subset=['frontLat', 'frontLon','upperAddrName', 'middleAddrName'])
        # 'news_title','news_summary' 컬럼 삭제
        final_df = final_df.drop(columns=['news_title', 'news_summary'])
    if final_df is not None:
        print("Length of final_df:", len(final_df))
    return final_df

def add_chucheon_category(**kwargs):
    final_df = kwargs['ti'].xcom_pull(task_ids='null_prepro_task')
    if final_df is not None:
        final_df['p_chucheon']= None
        print("Length of daily_df:", len(final_df))
    return final_df

# 'img_url' 길이가 1000 이상인 데이터는 삽입 불가이므로 그 데이터는 삭제
def img_url_1000_del(**kwargs):
    final_df = kwargs['ti'].xcom_pull(task_ids='add_chucheon_category_task')
    if final_df is not None:
        # 길이가 1000 이상인 행의 인덱스 조회
        long_index = final_df[final_df['img_url'].str.len() >= 1000].index
        # 조회된 인덱스 삭제
        final_df.drop(index=long_index, inplace=True)
        final_df.reset_index(drop=True, inplace=True)
    if final_df is not None:
        print("Length of final_df:", len(final_df))
    return final_df

# 전처리 마지막 단계 데이터 DB 적재
def news_final_put_db(**kwargs):
    final_df = kwargs['ti'].xcom_pull(task_ids='img_url_1000_del_task')
    if final_df is None:
        print("final_df is None. Skipping database insertion.")
        return
    else:
        final_df['news_id'] = final_df['news_id'].astype(str)
        engine = create_engine('mysql+mysqldb://user name:db pw@host ip:port num/db name')
        final_df['news_id'] = final_df['news_id'].astype(str)
        final_df.to_sql(name='News_table_final', con=engine, if_exists='append', index=False, dtype={'news_id': VARCHAR(255)})
        return final_df

# News_table_final 데이터에서 'news_keyword', 'news_feature', 'news_content','news_date' 컬럼 삭제 후 DB 적재
def stores_store_put_db(**kwargs):
    final_df = kwargs['ti'].xcom_pull(task_ids='news_final_put_db_task')
    if final_df is None:
        print("stores_store is None. Skipping database insertion.")
        return
    else:
        final_df.drop(columns=['news_keyword', 'news_feature', 'news_content','news_date'], inplace=True)
        engine = create_engine('mysql+mysqldb://user name:db pw@host ip:port num/db name')
        final_df.to_sql(name='stores_store', con=engine, if_exists='append', index=False)


con_database_news_table_prepro2_task = PythonOperator(
    task_id='con_database_news_table_prepro2_task',
    python_callable=con_database_news_table_prepro2,
    dag=dag
)

location_task = PythonOperator(
    task_id='location_task',
    python_callable=location,
    provide_context=True,
    dag=dag
)

null_prepro_task = PythonOperator(
    task_id='null_prepro_task',
    python_callable=null_prepro,
    provide_context=True,
    dag=dag
)

add_chucheon_category_task = PythonOperator(
    task_id='add_chucheon_category_task',
    python_callable=add_chucheon_category,
    provide_context=True,
    dag=dag
)

img_url_1000_del_task = PythonOperator(
    task_id='img_url_1000_del_task',
    python_callable=img_url_1000_del,
    provide_context=True,
    dag=dag
)

news_final_put_db_task = PythonOperator(
    task_id='news_final_put_db_task',
    python_callable=news_final_put_db,
    provide_context=True,
    dag=dag
)

stores_store_put_db_task = PythonOperator(
    task_id='stores_store_put_db_task',
    python_callable=stores_store_put_db,
    provide_context=True,
    dag=dag
)

con_database_news_table_prepro2_task >> location_task >> null_prepro_task >> add_chucheon_category_task >> img_url_1000_del_task >> news_final_put_db_task >> stores_store_put_db_task