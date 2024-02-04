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
*Укажите не менее чем по 5 дисциплин для каждого сезона.
Сохраните DataFrame в csv-файл, разделитель колонок табуляция, первая строка должна содержать название колонок.
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


