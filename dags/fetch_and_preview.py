from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_data(**kwargs):
    url = 'https://raw.githubusercontent.com/Ilias1313/Data_ing_1/refs/heads/main/dags/data.csv'
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=None, names=['Category', 'Price', 'Quantity'])
        
        # Conversion du DataFrame en chaîne JSON pour XCom
        json_data = df.to_json(orient='records')

        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to get data, HTTP status code: {response.status_code}')

def preview_data(**kwargs):
    # Récupérer les données JSON à partir de XCom
    json_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    df = pd.read_json(json_data)
    print(df.head())  # Afficher les premières lignes du DataFrame pour prévisualisation

def preview_data(**kwargs):
    output_data = Kwargs['ti'].xcom_pull(key='data',task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from Xcom')
    
    #Dataframe From JSON data
    df = pd.Data.frame(output_data)
    


default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 11, 25),
    'catchup': False
}

dag = DAG(
    'fetch_and_preview',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

get_data_from_url = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

preview_data_from_url = PythonOperator(
    task_id='preview_data',
    python_callable=preview_data,
    provide_context=True,
    dag=dag
)

get_data_from_url >> preview_data_from_url
