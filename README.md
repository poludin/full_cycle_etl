# Предисловие
Данный проект состоит из нескольких задач. Первая треть посвящена работе с реляционными БД - PostgreSQL, 
остальные относятся к стэку BigData, а именно к технологии Spark. Но при этом у всех заданий есть общий принцип – работа с данными, 
в большинстве с помощью SQL. И важно заметить, что хорошо уметь делать выборки и трансформации данных – это лишь полдела, 
так же нужно уметь выстраивать полноценный автономный ETL-процесс. То есть ещё важно развивать навык архитектурного / системного мышления. 
Это очень ценный навык в продуктовой и корпоративной разработке.

![36830ETL-Process-for-linkedin3](https://github.com/poludin/full_cycle_etl/assets/70154853/e8fd697a-5d76-457a-a295-f98fab2cb794)

# Легенда
В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были 
перенесены из старых БД в одну новую централизованную БД.  Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, 
небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. Недавно банку 
потребовалось построить отчёт по 101 форме. Те данные что остались в csv-файлах тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и 
управленческих сложностей, нужно рассчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) 
хранилища в СУБД Oracle / PostgreSQL.

# Перечень технологий:
- OS Ubuntu 20.04
- PostgreSQL 15.5
- Python 3.9
- Airflow 2.6
- DBeaver 23.3.1
- Talend 7.3
