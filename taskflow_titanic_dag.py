from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'my_self',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='dag_with_taskflow_api_v01',
     default_args=default_args,
     start_date=datetime(2023, 2, 27))
def hello_world_etl():
    pass

    create_titanic_dataset = BashOperator(
        task_id='download_titanic_dataset',
        bash_command='''TITANIC_FILE="titanic-{{ execution_date.int_timestamp }}.csv" && \
        wget https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv -O ${TITANIC_FILE} &&\
        hdfs dfs -mkdir -p /datasets/ && \
        hdfs dfs -put ${TITANIC_FILE} /datasets/ && \
        rm ${TITANIC_FILE} &&\
        echo "/datasets/${TITANIC_FILE}"
        '''
    )

    drop_hive_table = HiveOperator(
        task_id='drop_hive_table',
        hql='DROP TABLE titanic'
    )

    create_hive_table = HiveOperator(
        task_id='create_hive_table',
        hql=''' CREATE TABLE IF NOT EXISTS titanic (Survived INT, Pclass INT, Name STRING, Sex STRING, Age INT, Sibs INT, Parch INT, Fare DOUBLE)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        TBLPROPERTIES('skip.header.line.count'='1');
        '''
    )

    load_titanic_dataset = HiveOperator(
        task_id='load_data_to_hive',
        hql='''LOAD DATA INPATH '{{ ti.xcom_pull(task_ids='download_titanic_dataset', key='return_value')}}' INTO TABLE titanic;'''
    )

    show_avg_fare = BashOperator(
        task_id='show_avg_fare',
        bash_command='''beeline -u jdbc:hive2://localhost:10000\
            -e "SELECT Pclass, avg(Fare) FROM titanic GROUP BY Pclass;"  | tr "\n" ":" '''
    )

    def format_message(**kwargs):
        flat_message = kwargs['ti'].xcom_pull(task_ids='show_avg_fare', key='return_value')
        message = flat_message.replace(';', '\n')
        kwargs['ti'].xcom_push(key='telegram_message', value=message)

    prepare_message = PythonOperator(
        task_id='prepare_message',
        python_callable=format_message,
    )

    send_result = TelegramOperator(
        task_id='send_success_message_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='568462623',
        text='''Pipline {{ execution_date.int_timestamp }} is done. Result:
        {{ ti.xcom_pull(task_ids='prepare_message', key='telegram_message') }}'''
    )

    create_titanic_dataset >> drop_hive_table >> create_hive_table >> load_titanic_dataset >> show_avg_fare >> prepare_message >> send_result

greet_dag = hello_world_etl()