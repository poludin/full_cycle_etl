# Задача
Выполнив предыдущие 2 задачи нам удалось рассчитать отчётную форму 101. Менеджер проекта доволен, ЦБ получил отчёт, руководство банка тоже довольно. Теперь необходимо выгрузить эти данные в формате – CSV, который бы позволит легко обмениваться данными между отделами.  
Напишим скрипт, который позволит выгрузить данные из витрины «dm.dm _f101_round_f» в csv-файл, первой строкой должны быть наименования колонок таблицы.  
Убедимся, что данные выгружаются в корректном формате и напишим скрипт, который позволит импортировать их обратно в БД. 
Поменяем пару значений в выгруженном csv-файле и загрузим его в копию таблицы 101-формы «dm.dm _f101_round_f_v2».

# Шаги:
# 0. Создадим копию таблицы dm.dm _f101_round_f dm - dm.dm _f101_round_f_v2
```sql
create table dm.dm_f101_round_f_v2 (
    rep_date 		                date,
    chapter 				char(1),
    ledger_account 			char(5),
    characteristic 			char(1),
    bal_in_rub				numeric(23, 8),
    bal_in_val	 			numeric(23, 8),
    bal_in_total			numeric(23, 8),
    turn_deb_rub			numeric(23, 8),
    turn_deb_val	 		numeric(23, 8),
    turn_deb_total	 		numeric(23, 8),
    turn_cre_rub 			numeric(23, 8),
    turn_cre_val	 		numeric(23, 8),
    turn_cre_total 			numeric(23, 8),
    bal_out_rub		 		numeric(23, 8),
    bal_out_val				numeric(23, 8),
    bal_out_total 			numeric(23, 8)
);
```
В эту таблицу будем загружать изменные данные с витирины dm.dm _f101_round_f.
# 1. Создадим скрипт для экспорта и импорта данных  
Для создания скрипта будем использовать Python Jupyter Notebook.  
Создадим файл task_1.3.ipynb. В нем создадим две функции - импорт и экспорт данных.

Сначала импортируем неоходимые библиотеки:
```python
import csv
import psycopg2
import logging
```
Определим  настройки подключения к базе данных:
```python
db_host = 'localhost'
db_port = '5432'
db_name = 'ns_petproject'
db_user = 'user'
db_password = 'password'
```
Создадим файл для логирования событий и определим настройки логирования:
```python
log_file = 'data_migration.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```
Создадим функцию для выгрузки данных из витрины в CSV-файл:
```python
def export_data_to_csv():
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()

        # Запрос для получения данных из витрины
        query = "SELECT * FROM dm.dm_f101_round_f"

        # Выполнение запроса
        cursor.execute(query)
        rows = cursor.fetchall()

        # Заголовки столбцов
        headers = [desc[0] for desc in cursor.description]

        # Запись данных в CSV-файл
        with open('data_export.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(rows)

        logging.info("Data exported to CSV successfully")

    except (Exception, psycopg2.Error) as error:
        logging.error("Error exporting data to CSV: " + str(error))

    finally:
        if conn:
            cursor.close()
            conn.close()
```
Вызовим функцию для выполнения выгрузки данных:
```python
export_data_to_csv()
```
В результате будет создан файл data_export.csv с данными из dm.dm _f101_round_f и запишиться лог в файл data_migration.log о успешной выгрузки данных:
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/b06e040e-99ba-401a-9f2d-4fd1070f3b3b)
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/81327b0d-5a47-46e9-bc1a-7d8395872b23)



