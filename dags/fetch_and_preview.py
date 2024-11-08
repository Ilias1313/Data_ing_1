from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime, timedelta
import json

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
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCom')
    
    # DataFrame depuis les données JSON
    df = pd.DataFrame(output_data)

    # Calculer les ventes totales
    df['Total'] = df['Price'] * df['Quantity']
    df = df.groupby('Category', as_index=False).agg({'Quantity': 'sum', 'Total': 'sum'})

    # Trier par ventes totales
    df = df.sort_values(by='Total', ascending=False)

    print(df[['Category', 'Total']].head(20))

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 11, 1),
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
    dag=dag
)

get_data_from_url >> preview_data_from_url
