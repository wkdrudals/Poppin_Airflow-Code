import re
import time
import pymysql
import requests
import pandas as pd
import numpy as np
from airflow import DAG
from openai import OpenAI
from sqlalchemy import text
from bs4 import BeautifulSoup
from airflow.models.dag import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from sqlalchemy import create_engine, VARCHAR, DATE
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from requests.exceptions import RequestException
pymysql.install_as_MySQLdb()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Daily_prepro_3',  
    default_args=default_args,  
    description='Daily_prepro_3',  
    start_date=datetime(2024, 3, 8),
    schedule_interval='40 2 * * *',
    tags=['preprocess_3'],
    catchup=False
)

def con_database_news_table_prepro2():
    connection = pymysql.connect(
        host='host ip',
        user='user name',
        password='db pw',
        database='db name',
        port= port name
    )
    connection_url = f"mysql+pymysql://user name:db pw@host ip:port num/db name"
    engine = create_engine(connection_url)
    query = "SELECT * FROM News_table_prepro2 WHERE news_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"
    daily_df_1 = pd.read_sql_query(query, connection)
    if daily_df_1.empty:  
            return None
    else:
        return daily_df_1

def not_open_date_delete(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='database_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    else:
        # 정보가 없을 경우 삭제
        daily_df_1 = daily_df_1.dropna(subset=['p_name', 'p_location', 'p_hashtag'], how='any')
        # 시작일 & 종료일 둘 다 없을 경우 삭제
        daily_df_1 = daily_df_1.dropna(subset=['p_startdate', 'p_enddate'], how='all')
        # 시작일이 종료일보다 이상인 행 삭제
        daily_df_1 = daily_df_1.drop(daily_df_1[daily_df_1['p_startdate'] > daily_df_1['p_enddate']].index)
        print("daily_df_1:", daily_df_1)
        print("Length of daily_df_1:", len(daily_df_1))
        return daily_df_1

# p_location에서 다중값 중 1개만 남기고 제거
def p_location_prepro(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='not_open_date_delete_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None

        for index, row in daily_df.iterrows():
            if ',' in row['p_location']:
                print(f"Index: {index}, Location: {row['p_location']}")
    return daily_df_1

# 첫 번째 값만 남기는 함수
def process_location_func(location):
    return location.split(',')[0]

def process_location(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='p_location_pre')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    else:
        # 각 행에 함수 적용
        daily_df_1['p_location'] = daily_df_1['p_location'].apply(process_location_func)
        return daily_df_1

# p_location에 "층"이 있다면 제거
def remove_floor_func(location):
    # "층"을 제거하는 함수
    return re.sub(r'\s\d+층', '', location)

def remove_floor(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='pro_location')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    else:
        # p_location 컬럼에 함수 적용하여 처리
        daily_df_1['p_location'] = daily_df_1['p_location'].apply(remove_floor_func)
        return daily_df_1

# 괄호 제거
def delete_bracket(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='re_floor')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    else:
        daily_df_1['p_location'] = daily_df_1['p_location'].str.replace(r'\(.*\)', '', regex=True)
        return daily_df_1

# 불용어 제거
def word_delete(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='del_br')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    else:
        # 'GS25', '백화점'이 포함된 행 삭제
        daily_df_1 = daily_df_1[~((daily_df_1['p_location'] == 'GS25') | (daily_df_1['p_location'] == '백화점'))]
        return daily_df_1


def all_cols_filter(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='word_del') 
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    # 필터링할 값들을 리스트로 지정
    filter_values = ['알 수 없음', '알 수 없음 (null)','데이터 없음', '해당 정보 없음', '해당 없음', '정보 없음', '정보없음','(정보 없음)', '없음', '알려지지 않음',
                '미상', '미정', '미정 (정보 없음)', '미기재',  '미기재 (null)', '(미기재)', 'N/A', 'null', 'null (정보 없음)', '없음 (null)', '비공개 (null)', '-',
                '<NA>', 'NA', '불명 (null)','상세 내용 미포함']

    # 필터링된 값들 삭제
    daily_df_1 = daily_df_1[~daily_df_1['p_name'].isin(filter_values)]
    # 시작일 & 종료일 둘 다 없을 경우 삭제
    daily_df_1 = daily_df_1[~(daily_df_1['p_startdate'].isin(filter_values) & daily_df_1['p_enddate'].isin(filter_values))]
    daily_df_1 = daily_df_1[~daily_df_1['p_location'].isin(filter_values)]
    daily_df_1 = daily_df_1[~daily_df_1['p_hashtag'].isin(filter_values)]
    # 인덱스 리셋
    daily_df_1.reset_index(drop=True, inplace=True)
    return daily_df_1

# 중복된 팝업스토어 삭제
def dup_popupstore_del(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='all_cols_filter_task')
    if daily_df_1 is not None and not daily_df_1.empty and 'p_name' in daily_df_1.columns:
        # 'p_name'을 기준으로 그룹
        grouped_indices = daily_df_1.groupby('p_name').apply(lambda x: x.index.tolist())
        # 중복된 행이 있는 그룹만 선택
        duplicates = grouped_indices[grouped_indices.apply(len) > 1]

        # 인덱스가 가장 큰 행을 남기고 나머지 중복된 행은 삭제
        for indices in duplicates:
            max_index = max(indices)
            for index in indices:
                if index != max_index:
                    daily_df_1.drop(index, inplace=True)
        print("Length of daily_df_1:", len(daily_df_1))
        return daily_df_1
    else:
        print("No data or 'p_name' column found.")
        return None

# 전처리 3단계 데이터 DB 적재
def News_table_prepro3_put_DB(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='dup_popupstore_del_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping database insertion.")
        return None
    else:
        engine = create_engine('mysql+mysqldb://user name:db pw@host ip:port num/db name')
        daily_df_1['news_id'] = daily_df_1['news_id'].astype(str)
        daily_df_1.to_sql(name='News_table_prepro3', con=engine, if_exists='append', index=False, dtype={'news_id': VARCHAR(255)})
        return daily_df_1

database_task = PythonOperator(
    task_id='database_task',
    python_callable=con_database_news_table_prepro2,
    dag=dag
)

not_open_date_delete_task = PythonOperator(
    task_id='not_open_date_delete_task',
    python_callable=not_open_date_delete,
    provide_context=True,
    dag=dag
)

p_location_pre = PythonOperator(
    task_id='p_location_pre',
    python_callable=p_location_prepro,
    provide_context=True,
    dag=dag
)

pro_location = PythonOperator(
    task_id='pro_location',
    python_callable=process_location,
    provide_context=True,
    dag=dag
)

re_floor = PythonOperator(
    task_id='re_floor',
    python_callable=remove_floor,
    provide_context=True,
    dag=dag
)

del_br = PythonOperator(
    task_id='del_br',
    python_callable=delete_bracket,
    provide_context=True,
    dag=dag
)

word_del = PythonOperator(
    task_id='word_del',
    python_callable=word_delete,
    provide_context=True,
    dag=dag
)

all_cols_filter_task = PythonOperator(
    task_id='all_cols_filter_task',
    python_callable=all_cols_filter,
    provide_context=True,
    dag=dag
)

dup_popupstore_del_task = PythonOperator(
    task_id='dup_popupstore_del_task',
    python_callable=dup_popupstore_del,
    provide_context=True,
    dag=dag
)

News_table_prepro3_put_DB_task = PythonOperator(
    task_id='News_table_prepro3_put_DB_task',
    python_callable=News_table_prepro3_put_DB,
    provide_context=True,
    dag=dag
)

database_task >> not_open_date_delete_task >> p_location_pre>> pro_location >> re_floor >> del_br >> word_del >> all_cols_filter_task >> dup_popupstore_del_task >> News_table_prepro3_put_DB_task 