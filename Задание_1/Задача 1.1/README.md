Файлы источники - md_ledger_account_s.csv, md_account_d.csv, ft_balance_f.csv, ft_posting_f.csv, md_currency_d.csv, md_exchange_rate_d ([находятся тут](https://github.com/poludin/full_cycle_etl/tree/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_1/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0%201.1/data_sources)), необходимо с помощью ETL инструмента Talend загрузить в базу данных PostgreSQL.  

# Шаги:

# 0. Создание таблиц в базе данных для загрузки данных:

- В БД PostgreSQL с помощью DBeaver были созданы таблицы следующей [структуры](https://github.com/poludin/full_cycle_etl/blob/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_1/%D0%A1%D1%82%D1%80%D1%83%D0%BA%D1%82%D1%83%D1%80%D0%B0%20%D1%82%D0%B0%D0%B1%D0%BB%D0%B8%D1%86%20%D0%B4%D0%BB%D1%8F%20PostgreSQL.docx);
- SQL скрипт создания схем и таблиц [находится тут](https://github.com/poludin/full_cycle_etl/tree/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_1/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0%201.1/sql_scripts).

# 1. Предобработка файлов источников:

- Во всех файлах удалил первый столбец.
- Изменил формат даты с 31.12.2017 на 2017-12-31 - в PostgreSQL при создании таблицы нельзя изменить формат даты.
- Изменил кодировку md_ledger_account_s.csv с IBM866 на UTF-8 с помощью Блокнота++.
 
 В дальнейшем можно было бы реализовать предобработку данных с помощью Pandas/NumPY. Это быстрее, эффективнее и можно обрабатывать большие объемы данных. Конечно, со Sparkом не сравнится, но это лучше, чем в ручную перебирать данные. 

# 2. Подготовка:

- Установил Talend 7.3 на Ubuntu 20.04. Специфика установки была в выборе правильной версии Java.

# 3. Создание Talend Job:

- Создал новый Job Talend и указали имя ns_petproject.
  
![Screenshot from 2024-01-04 16-53-48](https://github.com/poludin/full_cycle_etl/assets/70154853/609f0445-278e-4e79-aa16-c84b31bda82e)

# 4. Добавление CSV Input компонентов:

- Добавил CSV Input компоненты для каждого файла данных источника.
- Настройте параметры каждого CSV Input компонента.  

Указал:
1. Имя источника.
2. Откуда его загружаем.
3. Кодировку, разделитель, хэдар(название колонок).
4. Указал какие колонки будут являться первичными ключами, тип данных колонок; могут принимать NULL или нет, длину данных.  
P.S. Пришлось менять в ручную названия колонок из верхнего регистра в нижний, потому что PostgreSQL выдавал ошибку и не хотел грузить данные (Перевести названия колонок в верхний регистр при создании таблицы - не помогло).

![Screenshot from 2024-01-04 16-56-41](https://github.com/poludin/full_cycle_etl/assets/70154853/70be6a9e-e98d-4247-87a4-1774f338f6ac)

![Screenshot from 2024-01-04 16-57-17](https://github.com/poludin/full_cycle_etl/assets/70154853/57512bf4-458c-4589-9c76-2da1bda010cf)

![Screenshot from 2024-01-04 16-57-41](https://github.com/poludin/full_cycle_etl/assets/70154853/1a640077-d9c9-486a-a62c-e332fd320173)

![Screenshot from 2024-01-04 17-10-06](https://github.com/poludin/full_cycle_etl/assets/70154853/50a1bd32-8bb7-4af0-a5c2-065c66f1451e)

![Screenshot from 2024-01-04 17-07-27](https://github.com/poludin/full_cycle_etl/assets/70154853/ed5e502a-7733-463a-8349-ad0435f7f942)


# 5. Добавление Output компонента:

- Создал соединение с схемами DS(детальный слой) и LOGS(логирование данных) PostgreSQL в Talend.  

Указал:
1. Имя соединения с определенной схемой.
2. Какой тип БД используем, версию, логин, пароль, сервер, к какой БД подключаемся, схему.
3. В конце сделали тест подключения к БД.
   
![Screenshot from 2024-01-05 20-50-59](https://github.com/poludin/full_cycle_etl/assets/70154853/e93195ec-c0c4-47cb-a503-a754ee21d039)

![Screenshot from 2024-01-04 17-13-04](https://github.com/poludin/full_cycle_etl/assets/70154853/f7562189-61a1-4acf-a196-7def02fefbde)

![Screenshot from 2024-01-05 20-51-10](https://github.com/poludin/full_cycle_etl/assets/70154853/c1662fbc-29dc-4f84-8438-ca9fbb735b80)

![Screenshot from 2024-01-05 20-51-34](https://github.com/poludin/full_cycle_etl/assets/70154853/c041d282-8092-4ee8-98bd-7be28b521176)

![Screenshot from 2024-01-05 20-51-38](https://github.com/poludin/full_cycle_etl/assets/70154853/8b6568f8-9cfd-42b2-90b3-23e05e4dfdad)

![Screenshot from 2024-01-05 20-51-40](https://github.com/poludin/full_cycle_etl/assets/70154853/e89bcd34-a76a-4ac4-b405-662d1839d927)

# 6. Добавление компанентов:

- Добавил файл источник ft_balance_f с помощью компонента tFileInputDelimited.

![Screenshot from 2024-01-05 20-53-25](https://github.com/poludin/full_cycle_etl/assets/70154853/d2d93fdc-9c1e-4cbc-ae6c-7e77dbeb355f)

![Screenshot from 2024-01-05 20-53-29](https://github.com/poludin/full_cycle_etl/assets/70154853/c353fb67-d666-4c15-b8ef-4e4c6ba19047)

- Добавил БД PostgreSQL, в которую будем грузить данные из источника ft_balance_f. Использовал компонент tDBOutput(PostgreSQL).

![Screenshot from 2024-01-05 20-53-58](https://github.com/poludin/full_cycle_etl/assets/70154853/ec048630-2545-4691-81ba-ef37d2abfe33)

![Screenshot from 2024-01-05 20-54-02](https://github.com/poludin/full_cycle_etl/assets/70154853/16fff2fb-854a-42eb-a46f-91f36194ad1a)

- Проверил куда будут грузиться данные, в какую схему и таблицу + указали как будут записываться данные.
В нашем случае - Insert and update.

![Screenshot from 2024-01-05 20-55-02](https://github.com/poludin/full_cycle_etl/assets/70154853/f54f653f-6f95-4371-b3f5-407056f923f6)

- Соединил источник и БД.

![Screenshot from 2024-01-05 20-55-27](https://github.com/poludin/full_cycle_etl/assets/70154853/3246ce51-05b9-4fd6-acba-b98d73e7e467)

- Добавил компонент tSleep - таймер на 5 секунд.

![Screenshot from 2024-01-05 20-55-58](https://github.com/poludin/full_cycle_etl/assets/70154853/bb915336-2e15-4a7d-8d68-dbec8933226b)

- Добавил компонент для сбора логов tDBRow(PostgreSQL) в начало ETL, чтобы фиксировать начало процесса. 

![Screenshot from 2024-01-05 20-59-04](https://github.com/poludin/full_cycle_etl/assets/70154853/4a1f091c-b9eb-4478-9786-d3dd2b0ae114)

- Проверил, куда будут грузиться данные, в какую схему и таблицу + добавили запрос в виде SQL кода, как и что будет записываться в базу данных.

![Screenshot from 2024-01-05 20-59-22](https://github.com/poludin/full_cycle_etl/assets/70154853/f87256ef-6c6d-49e1-814d-82a4456bac06)

- Query отображается в виде кирпичей (скорее всего проблемы с кодировкой). Поэтому пришлось написать готовый запрос в DBeaver, оттуда его скопировать и вставить уже в Talend в Query запросе. 

![Screenshot from 2024-01-05 20-59-32](https://github.com/poludin/full_cycle_etl/assets/70154853/6cceabc9-c9c9-4cd9-8c30-f8d5094cb220)

![Screenshot from 2024-01-05 20-59-37](https://github.com/poludin/full_cycle_etl/assets/70154853/fd3abc5a-e264-437c-9dc4-abe2b5ad3527)

- Добавил компонент для сбора логов tDBRow(PostgreSQL) в конец ETL, чтобы фиксировать окончание процесса. 

![Screenshot from 2024-01-05 21-00-37](https://github.com/poludin/full_cycle_etl/assets/70154853/6d9098b1-3673-4c69-93e4-6710430533ab)

# 7. Запуск Job:

 - Запустили Job нашего проекта.

 ![Screenshot from 2024-01-05 21-00-41](https://github.com/poludin/full_cycle_etl/assets/70154853/55eed898-f7bd-4acf-a22d-e785e50258a6)

- Наш Job прошел без ошибок, везде ОК и мы видим, что connect и disconnect прошел без ошибок.

![Screenshot from 2024-01-05 21-00-52](https://github.com/poludin/full_cycle_etl/assets/70154853/a261934b-bff3-4dc4-b33e-eab16eb2fd68)

# 8. Добавим аналогично остальные компоненты для всех источников данных:

- Вот что у нас получилось.

![Screenshot from 2024-01-05 20-49-53](https://github.com/poludin/full_cycle_etl/assets/70154853/f547d81d-0439-45ba-b91c-d41c33d6c805)

# 9. Проверка результатов:

- Данные успешно загружены в PostgreSQL.

![Screenshot from 2024-01-05 20-45-38](https://github.com/poludin/full_cycle_etl/assets/70154853/d8fd3b9f-c7fd-4210-bb65-969059a9791b)

![Screenshot from 2024-01-05 20-46-15](https://github.com/poludin/full_cycle_etl/assets/70154853/4b8dff40-26d6-49cb-8498-918b33a7ec62)

![Screenshot from 2024-01-05 20-47-27](https://github.com/poludin/full_cycle_etl/assets/70154853/40e5398f-c009-495f-addf-2bb3e37d9b64)

![Screenshot from 2024-01-05 20-47-51](https://github.com/poludin/full_cycle_etl/assets/70154853/240f85ff-8f03-43e4-842e-89738d289da3)

![Screenshot from 2024-01-05 20-48-14](https://github.com/poludin/full_cycle_etl/assets/70154853/90e45c01-b0f6-4e32-8c5c-b17658641e71)

![Screenshot from 2024-01-05 20-48-35](https://github.com/poludin/full_cycle_etl/assets/70154853/c2f34362-fe05-46ae-b3c8-590066962f09)

![Screenshot from 2024-01-05 20-48-54](https://github.com/poludin/full_cycle_etl/assets/70154853/ce30a0df-6385-4b57-9ca6-ce1e261e0ead)

# 10. Добавим автоматизацию с помощью Airflow:

- Установил Airflow. Дефолтная БД при установки Airflow -  SQLLite, медленная, с ограниченной памятью и не может принимать параллельные процессы. Поэтому я переподключил Airflow к базе данных PostgreSQL (стал работать быстрее и могут записываться параллельные процессы).
- Создал папку DAGS в папке Airflow. В ней будем создавать Dags.
- В Talend сделал Build Job своего проекта, чтобы было что запускать Airflow.
- Перенес весь Build Job проекта в папку Airflow/dags(перенес весь, потому что если копировал один bash файл запуска sh, то Airflow не мог запскать проект).
- У меня был уже установлен Питон, поэтому я только загрузил необходимые библиотеки для работы с Airflow и DAG.
- Создал DAG для нашего ETL процесса(создал 2 файла, отличаются наполнением, один более подробный)([посмотреть тут](https://github.com/poludin/full_cycle_etl/tree/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_1/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0%201.1/airflow_dags)).
  
![Screenshot from 2024-01-07 00-33-45](https://github.com/poludin/full_cycle_etl/assets/70154853/99288f81-5150-4b72-8e30-4aec8fcd0f85)

![Screenshot from 2024-01-07 00-34-11](https://github.com/poludin/full_cycle_etl/assets/70154853/f5b9409e-7cd7-49d7-bd2d-12645b13e9dc)

- Запустил DAGs - они успешно отработали и записали данные в БД.

  ![Screenshot from 2024-01-07 00-15-37](https://github.com/poludin/full_cycle_etl/assets/70154853/23957e84-01f5-4f36-b14e-8ce27d92ab81)

 ![Screenshot from 2024-01-07 00-15-49](https://github.com/poludin/full_cycle_etl/assets/70154853/833f9cdb-5a02-4647-8a76-b286849522c3)

![Screenshot from 2024-01-07 00-34-04](https://github.com/poludin/full_cycle_etl/assets/70154853/284c3bca-ed28-4e37-85e8-b14d96012208)

# 11. Полностью Job Talend можно [посмотреть тут](https://github.com/poludin/full_cycle_etl/tree/main/%D0%97%D0%B0%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5_1/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0%201.1/talend).

# 12. Видео по созданию/работе Job Talend и Airflow:

- [Видео ETL Job Talend + Airlow](https://drive.google.com/file/d/1vhhl8qd4QAdGE2I7xUmgn6c2bOdbbYV-/view?usp=sharing). 
