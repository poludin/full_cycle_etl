# Задача
Выполнив предыдущие 2 задачи нам удалось рассчитать отчётную форму 101. Менеджер проекта доволен, ЦБ получил отчёт, руководство банка тоже довольно. Теперь необходимо выгрузить эти данные в формате – CSV, который бы позволит легко обмениваться данными между отделами.  
Напишим скрипт, который позволит выгрузить данные из витрины «dm.dm _f101_round_f» в csv-файл, первой строкой должны быть наименования колонок таблицы.  
Убедимся, что данные выгружаются в корректном формате и напишим скрипт, который позволит импортировать их обратно в БД. 
Поменяем пару значений в выгруженном csv-файле и загрузим его в копию таблицы 101-формы «dm.dm _f101_round_f_v2».

# Шаги:
# 0. Создадим копию таблицы dm.dm _f101_round_f - dm.dm _f101_round_f_v2
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
В эту таблицу будем загружать изменные данные с витирины «dm.dm _f101_round_f».

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
Вызовим функцию для выполнения выгрузки данных из базы данных:
```python
export_data_to_csv()
```
В результате будет создан файл data_export.csv с данными из dm.dm _f101_round_f и запишиться лог в файл data_migration.log о успешной выгрузки данных:
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/b06e040e-99ba-401a-9f2d-4fd1070f3b3b)
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/81327b0d-5a47-46e9-bc1a-7d8395872b23)

Изменим пару значений в выгруженном файле data_export.csv:
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/9bfbc8c7-7d4a-4f07-ab4e-d663a52c95d2)

Импортрируем данные из CSV-файла в таблицу dm.dm _f101_round_f_v2 с помощью функции:
```python
def import_data_from_csv():
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()

        # Очистка таблицы перед импортом данных
        cursor.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")

        # Чтение данных из CSV-файла
        with open('data_export.csv', 'r') as f:
            cursor.copy_expert("COPY dm.dm_f101_round_f_v2 FROM STDIN WITH (FORMAT CSV, HEADER)", f)
    
        conn.commit()
        logging.info("Data imported from CSV successfully")

    except (Exception, psycopg2.Error) as error:
        logging.error("Error importing data from CSV: " + str(error))

    finally:
        if conn:
            cursor.close()
            conn.close()
```
Вызовим функцию для выполнения загрузки данных в базу данных:
```python
import_data_from_csv()
```
В результате измененные данные загрузятся в базу данных в таблицу dm.dm _f101_round_f_v2 и запишиться лог в файл data_migration.log о успешной загрузки данных:
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/eb7d7625-e0ac-4f3b-bce9-144988f4b616)
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/7285a5fc-7ca6-48e9-9912-d500de785c65)

Файл с кодом можно посмотреть [тут](https://github.com/poludin/project_full_cycle_etl/blob/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_1/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0%201.3/task_1.3.ipynb).
