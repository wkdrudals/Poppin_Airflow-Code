import re
import time
import logging
import pymysql
import requests
import pandas as pd
import numpy as np
from airflow import DAG
from konlpy.tag import Okt
from sqlalchemy import text
from bs4 import BeautifulSoup
from airflow.models.dag import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from sqlalchemy import create_engine, VARCHAR, DATE
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from airflow.operators.python_operator import PythonOperator
from requests.exceptions import RequestException
from airflow.models import XCom
pymysql.install_as_MySQLdb()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout': timedelta(minutes=120)
}

dag = DAG(
    'Daily_prepro_1',  
    default_args=default_args,  
    description='Daily_prepro_1',  
    start_date=datetime(2024, 3, 8),
    schedule_interval='0 2 * * *',
    tags=['preprocess_1'],
    catchup=False
)

def con_database_isna_sum_news_url():
    connection = pymysql.connect(
        host='host ip',
        user='user name',
        password='db pw',
        database='db name',
        port= port num
    )
    connection_url = f"mysql+pymysql://user name:db pw@host ip:port num/db name"
    engine = create_engine(connection_url)
    query = "SELECT * FROM News_table WHERE news_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"
    daily_df = pd.read_sql_query(query, connection)
    if daily_df.empty:  
        return None
    else:
        daily_df.isna().sum()
        daily_df = daily_df.dropna(subset=['news_url'])
        return daily_df

# 뉴스 기사 img_url & 본문 전체 내용 크롤링
def content_img_url(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='con_database')
    content_list = []
    img_url_list = []

    batch_size = 10  
    news_ids = daily_df['news_id']

    for i in range(0, len(news_ids), batch_size):
        batch_news_ids = news_ids[i:i + batch_size]

        for news_id in batch_news_ids:
            retries = 3  
            while retries > 0:
                try:
                    url = f"https://www.bigkinds.or.kr/news/detailView.do?docId={news_id}&returnCnt=1&sectionDiv=1000"
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                        "Referer": f"https://www.bigkinds.or.kr/v2/news/newsDetailView.do?newsId={news_id}"
                    }
                    r = requests.get(url, headers=headers)
                    r.raise_for_status()  
                    r_json = r.json()

                    if 'detail' in r_json:
                        news_content = r_json['detail'].get('CONTENT')
                        img_url = r_json['detail'].get('IMAGES')
                    else:
                        logger.warning("JSON response doesn't contain the expected structure.")
                        news_content = None
                        img_url = None

                    content_list.append(news_content)
                    img_url_list.append(img_url)
                    break 

                except RequestException as e:
                    logger.warning(f"API 호출 오류: {e}")
                    retries -= 1  
                    time.sleep(3)  

    daily_df['news_content'] = content_list
    daily_df['img_url'] = img_url_list
    return daily_df

# 'news_content' 값의 특수문자 제거
def clean_text_func(text):
    if not isinstance(text, str):
        return ''  
    clean_html = re.sub('<.*?>', '', text)
    clean_punctuation = re.sub(r'[^\w\s\.\?\!]', '', clean_html)
    clean_text = ' '.join(clean_punctuation.split())
    return clean_text


def clean_text_df(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='content_img')
    daily_df['news_content'] = daily_df['news_content'].apply(clean_text_func)
    return daily_df

# 중복된 'news_id'제거
def dupl_news_id_delete(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='clean_text_f')
    daily_df = daily_df.drop_duplicates(subset='news_id')
    return daily_df

# 형태소 분석기
def morphological_preprocessing(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='dup_news_id_del')
    okt = Okt()
    def tokenizer(raw, pos=["Noun", "Alpha", "Number"], stopword=[], min_length=1):
        return [
            word for word, tag in okt.pos(
                raw,
                norm=True,
                stem=True
            )
            if len(word) >= min_length and tag in pos and word not in stopword
        ]
    return daily_df


def count_vectorize(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='morphological_prepro')

    if daily_df.empty:
        return None
# 데이터프레임에 기사가 1개일 경우 그대로 반환
    if len(daily_df) == 1:
        return daily_df
# 기사 본문에 나오는 단어의 빈도 계산
    vectorize = CountVectorizer(
        tokenizer=lambda x: x,
        preprocessor=lambda x: x,
        min_df=2
    )
    X = vectorize.fit_transform(daily_df['news_content'])
    # 코사인 유사도 계산
    cos_sim = cosine_similarity(X)
# 코사인 유사도가 0.9 이상인 경우를 중복으로 판단
    dup_idx = np.where(cos_sim > 0.9)
    # 중복 기사 인덱스와 제목 출력 및 중복 기사 제거
    to_drop = []
    for i in range(len(dup_idx[0])):
        if dup_idx[0][i] != dup_idx[1][i]:  # 자기 자신과의 유사도는 제외
            # 먼저 게시된 기사를 남기고 싶으므로 인덱스가 더 작은 행을 제거
            to_remove = min(dup_idx[0][i], dup_idx[1][i])
            to_drop.append(to_remove)

    if not to_drop:
        return daily_df
    # 중복 기사 제거
    daily_df = daily_df.drop(to_drop)
    # 인덱스 리셋
    daily_df = daily_df.reset_index(drop=True)

    return daily_df

# 'news_content'에 'news_date' 추가 (openai 분석에 이용하도록)
def daily_df_prepro(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='count_vectorized')
    daily_df['news_content'] = daily_df['news_date'].astype(str) + ' ' + daily_df['news_content']
    return daily_df

# 전처리 1단계 데이터 DB 적재
def News_table_prepro1_put_DB(**kwargs):
    daily_df = kwargs['ti'].xcom_pull(task_ids='daily_df_pre')
    if daily_df is None:
        print("daily_df is None. Skipping database insertion.")
        return None
    else:
        engine = create_engine('mysql+mysqldb://user name:db pw@host ip:port num/db name')
        daily_df['news_id'] = daily_df['news_id'].astype(str)
        daily_df.to_sql(name='News_table_prepro1', con=engine, if_exists='append', index=False, dtype={'news_id': VARCHAR(255)})
        return daily_df

con_database = PythonOperator(
    task_id='con_database',
    python_callable=con_database_isna_sum_news_url,
    dag=dag
)

content_img = PythonOperator(
    task_id='content_img',
    python_callable=content_img_url,
    provide_context=True,
    dag=dag
)

clean_text_f = PythonOperator(
    task_id='clean_text_f',
    python_callable=clean_text_df,
    provide_context=True,
    dag=dag
)

dup_news_id_del = PythonOperator(
    task_id='dup_news_id_del',
    python_callable=dupl_news_id_delete,
    provide_context=True,
    dag=dag
)

morphological_prepro = PythonOperator(
    task_id='morphological_prepro',
    python_callable=morphological_preprocessing,
    provide_context=True,
    dag=dag
)

count_vectorized = PythonOperator(
    task_id='count_vectorized',
    python_callable=count_vectorize,
    provide_context=True,
    dag=dag
)

daily_df_pre = PythonOperator(
    task_id='daily_df_pre',
    python_callable=daily_df_prepro,
    provide_context=True,
    dag=dag
)

News_table_prepro1_db = PythonOperator(
    task_id='News_table_prepro1_db',
    python_callable=News_table_prepro1_put_DB,
    provide_context=True,
    dag=dag
)

con_database >> content_img >> clean_text_f >> dup_news_id_del >> morphological_prepro >> count_vectorized >> daily_df_pre >> News_table_prepro1_db 