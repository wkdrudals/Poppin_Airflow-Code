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
    'Daily_prepro_2',  
    default_args=default_args,  
    description='Daily_prepro_2',  
    start_date=datetime(2024, 3, 8),
    schedule_interval='20 2 * * *',
    tags=['preprocess_2nd_OpenAi'],
    catchup=False
)

def con_database_news_table_prepro1():
    connection = pymysql.connect(
        host='host ip',
        user='user name',
        password='db pw',
        database='db name',
        port= port name
    )
    connection_url = f"mysql+pymysql://user name:db pw@host ip:port num/db name"
    engine = create_engine(connection_url)
    query = "SELECT * FROM News_table_prepro1 WHERE news_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"
    daily_df_1 = pd.read_sql_query(query, connection)
    if daily_df_1.empty:  
            return None
    else:
        return daily_df_1

# 팝업 스토어 이름/시작일/종료일/위치/해시태그 추출
def openai(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='con_database_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    # 데이터를 10개씩 처리하도록 분할하여 리스트에 저장
    chunks = [daily_df_1[i:i + 10] for i in range(0, len(daily_df_1), 10)]
    # OpenAI API 설정
    client = OpenAI(api_key='api key')

    # 각 분할된 데이터에 대해 처리
    summaries = []
    for chunk in chunks:
        chunk_summaries = []
        # 각 분할된 데이터로부터 content를 가져와 API에 전달하고 요약 정보 추출
        for index, row in chunk.iterrows():
            message = [
                {
                    "role": "system",
                    "content": """
                    Your objective is to summarize crucial details extracted from the article's upload date, title, and content,
                    tailored for OpenAPI usage. The summary should encompass name of the popup store, start date of the
                    popup store (in YYYY-MM-DD format), end date of the popup store (in YYYY-MM-DD format), specific location
                    details of the popup store, hashtags (#Hashtag format) ,excluding 'Popup store' or '팝업스토어'.
                    However, the response format should be as follows: Popup store name:, Popup store start date:,
                    Popup store end date:, Popup store location:, Hashtags: each followed by the respective information.

                    Ensure that the hashtags must contain information about the popup store excluding and the count of the hashtags must remain fixed at five. When extracting a location, extract detailed location
                    information such as building name or address name. In case of unavailability, please indicate a null value.
                    Responses should be in Korean.
                    """
                },
                {
                    "role": "user",
                    "content": row['news_content'][:2500] # 메시지 길이를 2000자로 제한하여 전달
                }
            ]
            # OpenAI API에 요청하여 응답 받기
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=message,
                temperature=0.7,
                max_tokens=1000,
                top_p=1
            )
            # 응답에서 요약 정보 추출
            summary = response.choices[0].message.content
            # 추출된 요약 정보를 리스트에 추가
            chunk_summaries.append(summary)
        
        summaries.extend(chunk_summaries)
    # 데이터프레임에 새로운 컬럼 'news_summary'로 추가
    daily_df_1['news_summary'] = summaries
    print("Length of daily_df_1:", len(daily_df_1))
    return daily_df_1


# daily_df에 새 컬럼 추가
# p_name: 팝업스토어 이름 
# p_startdate: 팝업스토어 시작일 
# p_enddate: 팝업스토어 종료일 
# p_location: 팝업스토어 위치 
# p_hashtag: 해시태그 
def add_col(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='openai_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    daily_df_1.loc[:, 'p_name'] = daily_df_1['news_summary'].apply(lambda x: re.search(r'(팝업스토어 이름|팝업 스토어 이름|팝업스토어 명|Popup store name): ([^,\n]*)', x).group(2) if re.search(r'(팝업스토어 이름|팝업 스토어 이름|팝업스토어 명|Popup store name): ([^,\n]*)', x) else '<NA>')
    daily_df_1.loc[:, 'p_startdate'] = daily_df_1['news_summary'].apply(lambda x: re.search(r'(팝업스토어 시작일|팝업 스토어 시작일|팝업스토어 시작 날짜|팝업 스토어 시작 날짜|Popup store start date): ([^,\n]*)', x).group(2) if re.search(r'(팝업스토어 시작일|팝업 스토어 시작일|팝업스토어 시작 날짜|팝업 스토어 시작 날짜|Popup store start date): ([^,\n]*)', x) else '<NA>')
    daily_df_1.loc[:, 'p_enddate'] = daily_df_1['news_summary'].apply(lambda x: re.search(r'(팝업스토어 종료일|팝업 스토어 종료일|팝업스토어 종료 날짜|팝업 스토어 종료 날짜|Popup store end date): ([^,\n]*)', x).group(2) if re.search(r'(팝업스토어 종료일|팝업 스토어 종료일|팝업스토어 종료 날짜|팝업 스토어 종료 날짜|Popup store end date): ([^,\n]*)', x) else '<NA>')
    daily_df_1.loc[:, 'p_location'] = daily_df_1['news_summary'].apply(lambda x: re.search(r'(팝업스토어 위치|팝업 스토어 위치|Popup store location): ([^,\n]*)', x).group(2) if re.search(r'(팝업스토어 위치|팝업 스토어 위치|Popup store location): ([^,\n]*)', x) else '<NA>')
    daily_df_1.loc[:, 'p_hashtag'] = daily_df_1['news_summary'].apply(lambda x: re.search(r'(해시태그|Hashtag|Hashtags): (.*)', x).group(2) if re.search(r'(해시태그|Hashtag|Hashtags): (.*)$', x) else '<NA>')
    print("Length of daily_df_1:", len(daily_df_1))
    return daily_df_1
    
# 해외 국가 데이터 불러오기
def daily_prepro_second(**kwargs):
    file_path = "/opt/airflow/dags/worldcities_15.csv" 
    try:
        world_df = pd.read_csv(file_path, dtype=str)
        return world_df
    except FileNotFoundError:
        raise FileNotFoundError("worldcities_15.csv 파일을 찾을 수 없습니다.")

# 해외국가 데이터 삭제
def delete_col(**kwargs):
    ti = kwargs['ti']
    daily_df_1 = ti.xcom_pull(task_ids='add_col_task')
    world_df = ti.xcom_pull(task_ids='world_df_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    # daily_df의 'p_location'와 worldcities_15.csv df의 'country','city'를 비교해 일치하는 행 삭제
    daily_df_1 = daily_df_1[~daily_df_1['p_location'].apply(lambda x: any(item.lower() in str(x).lower() for item in world_df['country'])
                                        or any(item.lower() in str(x).lower() for item in world_df['city']))]
    print("Length of daily_df_1:", len(daily_df_1))
    return daily_df_1

# 날짜 형식으로 변경하고 날짜 형식이 아닌 값은 NaN으로 처리
def process_date_func(date):
        try:
            pd.to_datetime(date, format='%Y-%m-%d')
            return date
        except ValueError:
            return pd.NA
# p_startdate 및 p_enddate 컬럼에 함수를 적용하여 처리
def process_date(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='delete_col_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping further processing.")
        return None
    daily_df_1['p_startdate'] = daily_df_1['p_startdate'].apply(process_date_func)
    daily_df_1['p_enddate'] = daily_df_1['p_enddate'].apply(process_date_func)
    print("daily_df_1:", daily_df_1)
    print("Length of daily_df_1:", len(daily_df_1))
    return daily_df_1

# 전처리 2단계 데이터 DB 적재
def News_table_prepro2_put_DB(**kwargs):
    daily_df_1 = kwargs['ti'].xcom_pull(task_ids='process_date_task')
    if daily_df_1 is None:
        print("daily_df_1 is None. Skipping database insertion.")
        return None
    else:
        engine = create_engine('mysql+mysqldb://user name:db pw@host ip:port num/db name')
        daily_df_1['news_id'] = daily_df_1['news_id'].astype(str)
        daily_df_1.to_sql(name='News_table_prepro2', con=engine, if_exists='append', index=False, dtype={'news_id': VARCHAR(255)})
        return daily_df_1

con_database_task = PythonOperator(
    task_id='con_database_task',
    python_callable=con_database_news_table_prepro1,
    dag=dag
)

openai_task = PythonOperator(
    task_id='openai_task',
    python_callable=openai,
    provide_context=True,
    dag=dag
)

add_col_task = PythonOperator(
    task_id='add_col_task',
    python_callable=add_col,
    provide_context=True,
    dag=dag
)

world_df_task = PythonOperator(
    task_id='world_df_task',
    python_callable=daily_prepro_second,
    provide_context=True,
    dag=dag
)

delete_col_task = PythonOperator(
    task_id='delete_col_task',
    python_callable=delete_col,
    provide_context=True,
    dag=dag
)

process_date_task = PythonOperator(
    task_id='process_date_task',
    python_callable=process_date,
    provide_context=True,
    dag=dag
)

News_table_prepro2_put_DB_task = PythonOperator(
    task_id='News_table_prepro2_put_DB_task',
    python_callable=News_table_prepro2_put_DB,
    provide_context=True,
    dag=dag
)

con_database_task >> openai_task >> add_col_task >> delete_col_task >> process_date_task >> News_table_prepro2_put_DB_task  
world_df_task.set_downstream(delete_col_task)