from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

def extract(**kwargs):
    df = pd.read_csv('C:\Users\Sudarshan\materials\employee.csv')  
    return df.to_dict()

def transform(**kwargs):
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='extract'))

    # Filter rows
    df = df[(df['Age'] >= 18) & (df['Age'] <= 65)]

    # Fill missing salaries
    df['Salary'].fillna(df['Salary'].mean(), inplace=True)

    # Format names
    df['Name'] = df['Name'].str.title()

    # Calculate tenure
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['Tenure'] = (pd.Timestamp('now') - df['Start Date']).dt.days // 365

    # Categorize salaries
    salary_bins = [0, 50000, 100000, float('inf')]
    salary_labels = ['Low', 'Medium', 'High']
    df['Salary Category'] = pd.cut(df['Salary'], bins=salary_bins, labels=salary_labels)

    # Standardize department names
    df['Department'] = df['Department'].replace({
        'Human Resources': 'HR',
        'Information Technology': 'IT'
    })

    return df.to_dict()

def load(**kwargs):
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='transform'))

    conn = psycopg2.connect(
        dbname='company_db',
        user='airflow',
        password='airflow',
        host='localhost'
    )
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO employees (name, age, department, start_date, salary, tenure, salary_category)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (row['Name'], row['Age'], row['Department'], row['Start Date'], row['Salary'], row['Tenure'], row['Salary Category'])
        )

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract, transform and load data into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract',
    provide_context=True,
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    provide_context=True,
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    provide_context=True,
    python_callable=load,
    dag=dag,
)

extract_task >> transform_task >> load_task
