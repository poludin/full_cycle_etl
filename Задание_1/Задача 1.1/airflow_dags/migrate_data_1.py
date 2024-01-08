import time
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {					#При создании DAG, устанавливает эти значения по умолчанию для всех задач в DAG
    'owner': 'airflow', 				#Определяет владельца DAG
    'depends_on_past': False,				#Определяет, зависят ли задачи в DAG от выполнения предыдущих задач
    'start_date': datetime(2024, 1, 6),			#Определяет дату, с которых начинается выполнение DAG
    'retries': 1,					#Определяет количество попыток повторного выполнения задачи в случае ошибки
	'retry_delay': timedelta(minutes = 2)		#Определяет интервал между повторными попытками выполнения задачи в случае ошибки, задача будет повторно выполнена через 5 минут после ошибки
}

test_dag = DAG(
    'data_migration_1',					#Уникальный идентификатор DAG
    default_args = default_args,			#Передает настройки по умолчанию для DAG
    schedule_interval = '@hourly'			#Интервал выполнения - каждый час
)

bash_task = BashOperator(
    task_id = 'bash_execute_script',			#Уникальный идентификатор задачи
    bash_command = 'sh -x ./ns_petproject_run.sh ',
    dag = test_dag					#Передеча параметров о времени запуска
)

bash_task						#Вызов задачи
