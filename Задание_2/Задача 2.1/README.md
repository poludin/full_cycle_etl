# Задачи

1) Сгенерировать DataFrame из трёх колонок (row_id, discipline, season) - олимпийские дисциплины по сезонам:
- row_id - число порядкового номера строки;
- discipline - наименование олимпийский дисциплины на английском (полностью маленькими буквами);
- season - сезон дисциплины (summer / winter).

*Указать не менее чем по 5 дисциплин для каждого сезона.  
Сохранить DataFrame в csv-файл, разделитель колонок табуляция, первая строка должна содержать название колонок.  
Данные должны быть сохранены в виде 1 csv-файла, а не множества маленьких.

2) Прочитать исходный файл "Athletes.csv".
Посчитать в разрезе дисциплин сколько всего спортсменов в каждой из дисциплинпринимало участие.
Результат сохранить в формате parquet.

3) Прочитать исходный файл "Athletes.csv".
Посчитать в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.
Получившийся результат нужно объединить с сгенерированным DataFrame из 1-го задания и в итоге вывести количество участников, только по тем дисциплинам, что есть в сгенерированном DataFrame.
Результат сохранить в формате parquet.

# Шаги:
# 0. Импортируем необходимые библиотеки для работы
```python
import os
import pyspark
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import  SparkContext
from pyspark.sql import functions as f
from pyspark.sql.functions import col
```

# 1. Выполним первое задание  
Сгенерировать DataFrame из трёх колонок (row_id, discipline, season) - олимпийские дисциплины по сезонам:

row_id - число порядкового номера строки;
discipline - наименование олимпийский дисциплины на английском (полностью маленькими буквами);
season - сезон дисциплины (summer / winter).
*Указать не менее чем по 5 дисциплин для каждого сезона.
Сохранить DataFrame в csv-файл, разделитель колонок табуляция, первая строка должна содержать название колонок.
Данные должны быть сохранены в виде 1 csv-файла а не множества маленьких.

Создадим DataFrame с олимпийскими дисциплинами:
```python
data = {
    'row_id': range(1, 11),
    'discipline': ['athletics', 'swimming', 'basketball', 'football', 'tennis', 'skiing', \
                   'hockey', 'figure_skating', 'snowboarding', 'curling'],
    'season': ['summer'] * 5 + ['winter'] * 5
}
```
Заполним DataFrame данными:
```python
{'row_id': range(1, 11),
 'discipline': ['athletics',
  'swimming',
  'basketball',
  'football',
  'tennis',
  'skiing',
  'hockey',
  'figure_skating',
  'snowboarding',
  'curling'],
 'season': ['summer',
  'summer',
  'summer',
  'summer',
  'summer',
  'winter',
  'winter',
  'winter',
  'winter',
  'winter']}
```
Запишем в переменную olympic_disciplines наши данные и выведим их:
```python
olympic_disciplines = pd.DataFrame(data)

olympic_disciplines
```

![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/b020b4db-5c26-46d6-9dd6-f4d289f310ec)

Сохраним DataFrame в CSV-файл:
```python
olympic_disciplines.to_csv('olympic_disciplines.csv', sep='\t', index=False)
```
# 2. Выполним второе задание 
Прочитать исходный файл "Athletes.csv".
Посчитать в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.
Результат сохранить в формате parquet.

Прочитаем файл Athletes.csv и запишим его DataFrame. Выведим результат первых 5 строк:
```python
df_athletes = pd.read_csv('Athletes.csv', sep=';')

df_athletes.head()
```
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/89c7b7c9-008e-4809-825d-eaff6fd3b910)

Подсчитаем количествао спортсменов в каждой дисциплине и выведим результат:
 ```python
result = df_athletes['Discipline'].value_counts().reset_index()
result.columns = ['discipline', 'count']

result
```
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/747e7ba9-6dac-464e-b3dd-a86dadd54efb)

Сохраним результат в формате Parquet:
```python
table = pa.Table.from_pandas(result)
pq.write_table(table, 'result.parquet')
```
# 3. Выполним третье задание
Прочитать исходный файл "Athletes.csv".
Посчитать в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.
Получившийся результат объединить с сгенерированным вами DataFrame из 1-го задания и в итоге вывести количество участников, только по тем дисциплинам, что есть в сгенерированном DataFrame.
Результат сохранить в формате parquet.

Прочитаем файл Athletes.csv и запишим его DataFrame. Выведим результат первых 5 строк:
```python
df_athletes = pd.read_csv('Athletes.csv', sep=';')

df_athletes.head()
```
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/89c7b7c9-008e-4809-825d-eaff6fd3b910)

Подсчитаем количествао спортсменов в каждой дисциплине и выведим результат:
 ```python
result = df_athletes['Discipline'].value_counts().reset_index()
result.columns = ['discipline', 'count']

result
```
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/747e7ba9-6dac-464e-b3dd-a86dadd54efb)

Преобразуем значения в колонке 'discipline' к нижнему регистру и выведим результат:
```python
result_athletes['discipline'] = result_athletes['discipline'].apply(lambda x: x.lower())

result_athletes
```
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/44701e85-07ab-4769-a659-816eed69b712)

Объединим результаты с DataFrame olympic_disciplines из первого задания с DataFrame result_athletes и выведим результаты:
```python
merged_result = pd.merge(olympic_disciplines, result_athletes, on='discipline')

merged_result
```
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/c57b0aeb-c1ab-409d-a6b3-6684862ba95d)

Вывод общего количества участников только по дисциплинам:
```python
otal_athletes = merged_result['count'].sum()
print("Общее количество участников:", total_athletes)

Общее количество участников: 4242
```
Сохранение итогового результата в формате Parquet:
```python
table = pa.Table.from_pandas(merged_result)
pq.write_table(table, 'final_result.parquet')

print("Итоговый результат сохранен в файл final_result.parquet")

Итоговый результат сохранен в файл final_result.parquet
```
Все данные можно посмотреть в директории "Задание 2.1", а полный скрипт файла [тут](https://github.com/poludin/project_full_cycle_etl/blob/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_2/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0%202.1/task_2.1.ipynb).
