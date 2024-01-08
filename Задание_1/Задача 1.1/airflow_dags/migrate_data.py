import time
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

data = DAG(
	dag_id = 'data_migration', 			#Уникальный идентификатор DAG
	schedule_interval = "*/20 * * * *",  		#Интервал выполнения - каждые 20 минут
	start_date = datetime(2024, 1, 6), 		#Определяет дату, с которых начинается выполнение DAG
	catchup = False) 				#Определяет, должны ли быть выполнены пропущенные задачи, если DAG запускается с отставанием

migrate = BashOperator(
	task_id = 'migrate', 				#Уникальный идентификатор задачи
	bash_command ='sh -x ./ns_petproject_run.sh ',
	dag = data)					#Передеча параметров о времени запуска

migrate 						#Вызов задачи
