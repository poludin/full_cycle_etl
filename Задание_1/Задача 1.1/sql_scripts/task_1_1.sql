/*
 * /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 */
/*
 * Создание пользователя и схемы logs(сбор логов) + предоставления привилегий 
 */

create user logs with password '*****';
create schema logs authorization logs;
grant all on schema logs to logs;
grant all privileges on all tables in schema logs to logs;


/*
 * Скрипт создания и изменения таблицы logs.load_logs
 */

create table logs.load_logs (
	row_change_time 	timestamp default current_timestamp,
	source 			varchar(50),
	action_datetime 	timestamp,
	action 			varchar(50)
);

--truncate table logs.load_logs; 	
select * from logs.load_logs order by action_datetime desc;

/*
 * Код для логов в Talend (не отображает корректно шрифт)
 */

insert into logs.load_logs(source, action_datetime, action) values ('ALL', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('ALL', current_timestamp, 'END');

insert into logs.load_logs(source, action_datetime, action) values ('ft_balance_f', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('ft_balance_f', current_timestamp, 'END');

insert into logs.load_logs(source, action_datetime, action) values ('ft_posting_f', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('ft_posting_f', current_timestamp, 'END');

insert into logs.load_logs(source, action_datetime, action) values ('md_account_d', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('md_account_d', current_timestamp, 'END');

insert into logs.load_logs(source, action_datetime, action) values ('md_currency_d', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('md_currency_d', current_timestamp, 'END');

insert into logs.load_logs(source, action_datetime, action) values ('md_exchange_rate_d', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('md_exchange_rate_d', current_timestamp, 'END');

insert into logs.load_logs(source, action_datetime, action) values ('md_ledger_account_s', current_timestamp, 'START');
insert into logs.load_logs(source, action_datetime, action) values ('md_ledger_account_s', current_timestamp, 'END');


/*
 * /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 */
/*
 * Создение пользователя и схемы DS(детальный слой) + предоставления привилегий  
 */

create user ds with password '*****';
create schema ds authorization ds;
grant all on schema ds to ds;
grant all privileges on all tables in schema ds to ds;


/*
 * Скрипт создания и изменения таблицы ds.ft_balance_f
 */

create table ds.ft_balance_f (
	on_date 	date not null,
	account_rk 	int not null,
	currency_rk 	int,
	balance_out 	float
);

alter table ds.ft_balance_f add primary key (on_date, account_rk);

--truncate table ds.ft_balance_f;
select * from ds.ft_balance_f order by account_rk;


/*
 * Скрипт создания и изменения таблицы ds.ft_posting_f
 */

create table ds.ft_posting_f (
	oper_date 		date not null,
	credit_account_rk 	int not null,
	debet_account_rk 	int not null,
	credit_amount 		float,
	debet_amount 		float
);

alter table ds.ft_posting_f add primary key (oper_date, credit_account_rk, debet_account_rk);

--truncate ds.ft_posting_f;
select * from ds.ft_posting_f order by oper_date;


/*
 * Скрипт создания и изменения таблицы ds.md_account_d
 */

create table ds.md_account_d (
	data_actual_date 	date not null,
	data_actual_end_date 	date not null,
	account_rk 		int not null,
	account_number 		varchar(20) not null,
	char_type 		varchar(1) not null,
	currency_rk 		int not null,
	currency_code 		varchar(3) not null
);

alter table ds.md_account_d add primary key (data_actual_date, account_rk);

--truncate ds.md_account_d;
select * from ds.md_account_d mad order by account_rk;


/*
 * Скрипт создания и изменения таблицы ds.md_currency_d
 */

create table ds.md_currency_d (
	currency_rk 		int not null,
	data_actual_date 	date not null,
	data_actual_end_date 	date,
	currency_code 		varchar(3),
	code_iso_char 		varchar(3)
);

alter table ds.md_currency_d add primary key (currency_rk, data_actual_date);

--truncate ds.md_currency_d;
select * from ds.md_currency_d order by currency_rk;


/*
 * Скрипт создания и изменения таблицы ds.md_exchange_rate_d
 */

create table ds.md_exchange_rate_d (
	data_actual_date 	date not null,
	data_actual_end_date 	date,
	currency_rk 		int not null,
	reduced_cource 		float,
	code_iso_num 		varchar(3)
);

alter table ds.md_exchange_rate_d add primary key (data_actual_date, currency_rk);

--truncate ds.md_exchange_rate_d;
select * from ds.md_exchange_rate_d order by currency_rk;


/*
 * Скрипт создания и изменения таблицы ds.md_ledger_account_s
 */

create table ds.md_ledger_account_s (
	chapter 				char(1),
	chapter_name 				varchar(16),
	section_number 				int,
	section_name 				varchar(22),
	subsection_name 			varchar(21),
	ledger1_account 			int,
	ledger1_account_name 			varchar(47),
	ledger_account 				int not null,
	ledger_account_name 			varchar(153),
	characteristic 				char(1),
	is_resident 				int,
	is_reserve 				int,
	is_reserved 				int,
	is_loan 				int,
	is_reserved_assets 			int,
	is_overdue 				int,
	is_interest 				int,
	pair_account 				varchar(5),
	start_date 				date not null,
	end_date 				date,
	is_rub_only 				int,
	min_term 				varchar(1),
	min_term_measure 			varchar(1),
	max_term 				varchar(1),
	max_term_measure 			varchar(1),
	ledger_acc_full_name_translit 		varchar(1),
	is_revaluation 				varchar(1),
	is_correct 				varchar(1)
);

alter table ds.md_ledger_account_s add primary key (ledger_account, start_date);

--truncate ds.md_ledger_account_s;
select * from ds.md_ledger_account_s order by ledger_account;


































