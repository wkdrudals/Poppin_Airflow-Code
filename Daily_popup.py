from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from airflow.models.dag import DAG
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine, VARCHAR, DATE
import pandas as pd
import time
import os
import pymysql
pymysql.install_as_MySQLdb()


CHROME_DRIVER_PATH = '/opt/airflow/chromedriver'
SERVICE_ARGS = ["--disable-notifications", "--disable-popup-blocking", "--disable-extensions"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Daily_popup',  
    default_args=default_args,  
    description='popup daily xlsx dowload and save to DB',  
    schedule_interval='0 1 * * *',
    tags=['Popup_store_xlsx to DB'],
    catchup=False
)

# end1 = DummyOperator(task_id='end1', dag=dag)

def setup_environment():
    os.environ["PATH"] += os.pathsep + "/opt/airflow/chromedriver"  
    print(datetime.now())

def popup_daily_excel():
    service = Service(executable_path=CHROME_DRIVER_PATH)
    options = webdriver.ChromeOptions()  
    options.add_argument("--headless")  
    options.add_argument("--no-sandbox")  
    options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--disable-gpu")  
    options.add_argument("--window-size=1920x1080")  
    options.add_experimental_option("excludeSwitches", ["enable-logging"])  
    options.add_experimental_option('useAutomationExtension', False)  
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})  
    options.add_argument("--disable-extensions")  
    driver = webdriver.Chrome(service=service, options=options)  

    driver.get("https://www.bigkinds.or.kr/")  
    driver.maximize_window()
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#header > div.hd-top > div > div.login-area > button.btn-login.login-modal-btn.login-area-before")
    login_btn.click()
    
    username_input = driver.find_element(By.CSS_SELECTOR, "#login-user-id")  
    username_input.send_keys("id")
    time.sleep(5)

    password_input = driver.find_element(By.CSS_SELECTOR, "#login-user-password")  
    password_input.send_keys("pw") 

    login_button = driver.find_element(By.CSS_SELECTOR, "#login-btn")  
    login_button.click()  
    time.sleep(10)  

    search_input = driver.find_element(By.CSS_SELECTOR, "#total-search-key")
    search_input.send_keys("팝업스토어")
    time.sleep(10)

    search_button = driver.find_element(By.CSS_SELECTOR, "#news-search-form > div > div.hd-srch.v2 > button")  
    search_button.click()
    time.sleep(10)

    period_button = driver.find_element(By.CSS_SELECTOR, "#news-search-form > div > div.hd-srch.v2 > div.srch-detail.v2 > div > div.tab-btn-wp1 > div.tab-btn-inner.tab1 > a")  
    period_button.click()  

    srch_button = driver.find_element(By.CSS_SELECTOR, "#srch-tab1 > div > div.radio-btn-wp.col6 > span:nth-child(1) > label")  
    srch_button.click()  

    yesterday = datetime.now() - timedelta(days=1)  
    yesterday_str = yesterday.strftime("%Y-%m-%d")  

    js_script = "document.querySelector('#search-begin-date').value='{}';"
    driver.execute_script(js_script.format(yesterday_str))

    js_script = "document.querySelector('#search-end-date').value='{}';"
    driver.execute_script(js_script.format(yesterday_str))

    apply_button = driver.find_element(By.CSS_SELECTOR, "#search-foot-div > div.foot-btn > button.btn.btn-search.news-search-btn.news-report-search-btn")  
    apply_button.click()  
    time.sleep(10)  

    collapse_button = driver.find_element(By.CSS_SELECTOR, "#collapse-step-3")  
    actions = ActionChains(driver)
    actions.move_to_element(collapse_button).perform()
    collapse_button.click()  
    time.sleep(10)  

    excel_button = driver.find_element(By.CSS_SELECTOR, "#analytics-data-download > div.btm-btn-wrp > button")  
    excel_button.click()  
    time.sleep(10)

    try:
        WebDriverWait(driver, 10).until(EC.alert_is_present())  
        alert = driver.switch_to.alert
        alert.accept()  
        print("알림 창이 닫혔습니다.")
    except:
        print("알림 창이 나타나지 않았습니다.")
    time.sleep(10)  

    driver.quit()
    time.sleep(10)  

def popup_excel_to_db():
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_filename = yesterday.strftime("%Y%m%d")
    source_xlsx = f'/opt/airflow/NewsResult_{yesterday_filename}-{yesterday_filename}.xlsx'

    try:
        df = pd.read_excel(source_xlsx, dtype="str")
    except FileNotFoundError:
        print("엑셀 파일이 존재하지 않습니다.")
        return

    if df.empty:
        print("엑셀 파일에 데이터가 없습니다.")
        return

    columns_to_drop = ['본문', '사건/사고 분류1', '사건/사고 분류2', '사건/사고 분류3', '인물', '위치', '기관', '언론사', '기고자', '분석제외 여부', '통합 분류1', '통합 분류2', '통합 분류3']
    df = df.drop(columns=columns_to_drop, inplace=False)
    df.rename(columns={'뉴스 식별자': 'news_id', '일자': 'news_date', '제목': 'news_title', '키워드': 'news_keyword', '특성추출(가중치순 상위 50개)': 'news_feature', 'URL' : 'news_url'}, inplace=True)
    
    engine = create_engine('mysql+mysqldb://user name:db pw@host ip:post num/db name')

    df['news_id'] = df['news_id'].astype(str)  
    df['news_date'] = pd.to_datetime(df['news_date'])  

    df.to_sql(name='News_table', con=engine, if_exists='append', index=False, dtype={'news_id': VARCHAR(255), 'news_date': DATE})


os_env_setup = PythonOperator(
    task_id='setup_environment',
    python_callable=setup_environment,
    dag=dag
)

download_task = PythonOperator(
    task_id='popup_daily_excel',
    python_callable=popup_daily_excel,
    dag=dag
)

process_task = PythonOperator(
    task_id='popup_excel_to_db',
    python_callable=popup_excel_to_db,
    dag=dag
)

os_env_setup >> download_task >> process_task 
