from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime
import requests


@dag(
    dag_id='31_jsonplaceholder_variable_example',
    start_date=datetime(2026, 6, 6),
    schedule=None,
    catchup=False,
    tags=['learning', 'api']
)
def user_api_variable_dag():


    @task
    def fetch_user_data():
        api_url = Variable.get(
            "source_api_url", 
            default_var="https://jsonplaceholder.typicode.com/users/1"
        )
        print(f"[INFO] Get Data from  URL : {api_url}")


        response = requests.get(api_url)
        
        if response.status_code == 200:
            user_data = response.json()
            
            user_name = user_data.get('name')
            user_email = user_data.get('email')
            user_city = user_data.get('address', {}).get('city')
            company_name = user_data.get('company', {}).get('name')
            
            print("--- Success ---")
            print(f"👤 Name: {user_name}")
            print(f"✉️ Email: {user_email}")
            print(f"📍 City: {user_city}")
            print(f"🏢 Company: {company_name}")
            print("---------------------------")
            
            return user_data
        else:
            raise Exception(f"Error: {response.status_code}")


    @task
    def process_user_data(data):
        print(f"[PROCESS] กำลังเตรียมนำข้อมูลของ {data.get('name')} ไปบันทึกเข้าสู่ระบบ...")


    raw_data = fetch_user_data()
    process_user_data(raw_data)


user_api_variable_dag()
