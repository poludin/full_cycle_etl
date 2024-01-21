# Задача
После того как детальный слой «DS» успешно наполнен исходными данными из файлов – нужно рассчитать витрину данных финансовой отчетности в слое «DM», которая будет хранить
таблицу оборотов и таблицу 101-й отчётной формы.  
Для выполнения задачи вам необходимо разработать таблицу оборотов «dm.dm_account_turnover_f» и таблицу 101-й отчётной формы «dm.dm_f101_round_f», и прежде всего нужно создать структуру таблицы и определить типы загружаемых данных. На вход вам пришли следующие бизнес-требования:  

Таблица оборотов (dm.dm_account_turnover_f) должна содержать следующие поля:
- Дата, на которую считаются обороты по счетам
- Номер счёта
- Оборот по дебету (в рублях и в тыс.рублей)
- Оборот по кредиту (в рублях и в тыс.рублей)

Таблица 101-й отчётной формы (dm.dm_f101_round_f) должна быть разработана в соответствие с требованиями по формату данных, предоставленными Центробанком и расчитываться по следующей логике:
- «Входящие остатки (в рублях/валют/итого)" - рассчитать как баланс по счетам на отчетную дату.
- «Обороты за отчетный период по дебету/кредиту (в рублях/валют/итого)» - рассчитать как сумму всех проводок за отчетную дату по дебету/кредиту.
- «Исходящие остатки (в рублях)»:
    1. Для счетов с признаком "Актив" и currency_code '643' рассчитать как Входящие остатки (в руб.) - Обороты по кредиту (в руб.) + Обороты по дебету (в руб.).
    2. Для счетов с признаком "Актив" и currency_code '810' рассчитать как Входящие остатки (в руб.) - Обороты по кредиту (в руб.) + Обороты по дебету (в руб.).
    3. Для счетов с признаком "Пассив" и currency_code '643' рассчитать как Входящие остатки (в руб.) + Обороты по кредиту (в руб.) - Обороты по дебету (в руб.).
    4. Для счетов с признаком "Пассив" и currency_code '810' рассчитать как Входящие остатки (в руб.) + Обороты по кредиту (в руб.) - Обороты по дебету (в руб.).
- «Исходящие остатки (в валюте)»:
    1. Для счетов с признаком "Актив" и currency_code не '643' и не '810' рассчитать как Входящие остатки (в валюте) - Обороты по кредиту (в валюте) + Обороты по дебету (в валюте).
    2. Для счетов с признаком "Пассив" и currency_code не '643' и не '810' рассчитать Входящие остатки (в валюте) + Обороты по кредиту (в валюте) - Обороты по дебету (в валюте).
- «Исходящие остатки (итого)» рассчитать как Исходящие остатки (в валюте) + Исходящие остатки (в рублях).

# Шаги:
# 0. Создание схемы dm(data mart - витрина данных)
```sql
create schema dm;
```
# 1. Создадим таблицу dm.dm_account_turnover_f
```sql
create table dm.dm_account_turnover_f (
    on_date			date,
    account_rk 			numeric,
    credit_amount 		numeric(23, 8),
    credit_amount_rub 		numeric(23, 8),
    debet_amount		numeric(23, 8),
    debet_amount_rub		numeric(23, 8)
);
```
# 2. Создадим прототип для таблицы dm.dm_account_turnover_f, рассчитывающий кредитовые и дебетовые обороты по счетам за каждый день января 2018 года
Будем инсертить данные в следующие столбцы:
```sql
insert into dm.dm_account_turnover_f (
	on_date,
	account_rk,
	credit_amount,
	credit_amount_rub,	
	debet_amount,
	debet_amount_rub
)
```
С помощью WITH рассчитаем 2 таблицы(таблица по кредитному обороту и таблица по дебетоваму обороту), из которых будем брать данные для инсерта и объединяться будут с помощью UNION ALL.  
Select для кредитного оборота:
```sql
select
		date(p.oper_date)    				as oper_date,
		er.data_actual_date 				as exchange_rate,
		er.reduced_cource 				as reduced_cource,
		p.credit_account_rk 				as account_rk,
		p.credit_amount 				as credit_amount,
		p.credit_amount * er.reduced_cource 		as credit_amount_rub,
		cast(null as numeric) 				as debet_amount,
		cast(null as numeric)				as debet_amount_rub
```
Select для дебетового оборота:
```sql
select
		date(p.oper_date)    				as oper_date,
		er.data_actual_date 				as exchange_rate,
		er.reduced_cource 				as reduced_cource,
		p.credit_account_rk 				as account_rk,
		cast(null as numeric) 				as credit_amount,
		cast(null as numeric)				as credit_amount_rub,
		p.debet_amount 					as debet_amount,
		p.debet_amount * er.reduced_cource 	        as debet_amount_rub
```
Данные будет брать из таблиц, которые объединили с помощью JOIN и по условию дату между 2018-01-01 и 2018-02-01:
```sql
from ds.ft_posting_f p
	join ds.md_account_d a on p.credit_account_rk = a.account_rk
	left join ds.md_exchange_rate_d er 
		on a.currency_rk = er.currency_rk
		and date(p.oper_date) >= '2018-01-01' and date(oper_date) < '2018-02-01'
	where date(p.oper_date) = er.data_actual_date
```
Далее вставляем данные из WITH в нужные колонки и группируем:
```sql
select
	t.oper_date						as on_date,
	t.account_rk 						as account_rk,
	sum(t.credit_amount) 					as credit_amount,
	sum(t.credit_amount_rub) 				as credit_amount_rub,
	sum(t.debet_amount) 					as debet_amount,
	sum(t.debet_amount_rub) 				as debet_amount_rub
from wt_turn t
group by
	t.oper_date,
	t.account_rk
```
В результате получим следующию таблицу:
![image](https://github.com/poludin/project_full_cycle_etl/assets/70154853/0bee1ba9-35f0-4385-b741-d7612db8ed81)

# 4. Создадим таблицу dm.dm_f101_round_f
```sql
create table dm.dm_f101_round_f (
    rep_date 				date,
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
# 4. Создадим прототип для таблицы dm.dm_f101_round_f
Таблица будет рассчитываться по следующей логике:  
- «Входящие остатки (в рублях/валют/итого)" - рассчитать как баланс по счетам на отчетную дату.
- «Обороты за отчетный период по дебету/кредиту (в рублях/валют/итого)» - рассчитать как сумму всех проводок за отчетную дату по дебету/кредиту.
- «Исходящие остатки (в рублях)»:
    1. Для счетов с признаком "Актив" и currency_code '643' рассчитать как Входящие остатки (в руб.) - Обороты по кредиту (в руб.) + Обороты по дебету (в руб.).
    2. Для счетов с признаком "Актив" и currency_code '810' рассчитать как Входящие остатки (в руб.) - Обороты по кредиту (в руб.) + Обороты по дебету (в руб.).
    3. Для счетов с признаком "Пассив" и currency_code '643' рассчитать как Входящие остатки (в руб.) + Обороты по кредиту (в руб.) - Обороты по дебету (в руб.).
    4. Для счетов с признаком "Пассив" и currency_code '810' рассчитать как Входящие остатки (в руб.) + Обороты по кредиту (в руб.) - Обороты по дебету (в руб.).
- «Исходящие остатки (в валюте)»:
    1. Для счетов с признаком "Актив" и currency_code не '643' и не '810' рассчитать как Входящие остатки (в валюте) - Обороты по кредиту (в валюте) + Обороты по дебету (в валюте).
    2. Для счетов с признаком "Пассив" и currency_code не '643' и не '810' рассчитать Входящие остатки (в валюте) + Обороты по кредиту (в валюте) - Обороты по дебету (в валюте).
- «Исходящие остатки (итого)» рассчитать как Исходящие остатки (в валюте) + Исходящие остатки (в рублях).

Будем инсертить данные в следующие столбцы:
```sql
insert into dm.dm_f101_round_f (
	rep_date,
	chapter,
	ledger_account,
	characteristic,
	bal_in_rub,
	bal_in_val,
	bal_in_total,
	turn_deb_rub,
	turn_deb_val,
	turn_deb_total,
	turn_cre_rub,
	turn_cre_val,
	turn_cre_total,
	bal_out_rub,
	bal_out_val,
	bal_out_total
)
```
Рассчитывать данные будем сразу в SELECT. Столбцы rep_date, chapter, ledger_account, characteristic рассчитывать не нужно. А с остальными столбцами нужно будет реализовывать логику рассчета.  
Реализуем логику - «Входящие остатки (в рублях/валют/итого)" - рассчитать как баланс по счетам на отчетную дату:
```sql
	--Входящие остатки в рублях
	sum(
		case
			when cur.currency_code in ('643', '810')
				then b.balance_out
			else 0
		end
		) 									as bal_in_rub,
	--Входящие остатки в валюте, переведенные в рубли
	sum(
		case
			when cur.currency_code not in ('643', '810')
				then b.balance_out * ech_r.reduced_cource
			else 0
		end
		) 									as bal_in_val,
	--Итого:Входящие остатки в рублях + Входящие остатки в валюте, переведенные в рубли 
	sum(
		case 
			when cur.currency_code in ('643', '810')
				then b.balance_out
			else b.balance_out * ech_r.reduced_cource
		end
		) 									as bal_in_total
```
Реализуем логику - «Обороты за отчетный период по дебету/кредиту (в рублях/валют/итого)» - рассчитать как сумму всех проводок за отчетную дату по дебету/кредиту:
```sql
	--Обороты за отчетный период по дебету в рублях
	sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.debet_amount_rub
			else 0
		end
	) 										as turn_deb_rub,
	--Обороты за отчетный период по дебету в валюте, переведенный в рубли 
	sum(
		case
			when cur.currency_code not in ('643', '810')
				then at.debet_amount_rub * ech_r.reduced_cource
			else 0
		end
		) 									as turn_deb_val,
	--Итого: Обороты за отчетный период по дебету в рублях + Обороты за отчетный период по дебету в валюте, переведенный в рубли 
	 sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.debet_amount_rub
			else at.debet_amount_rub * ech_r.reduced_cource
		end
		) 									as turn_deb_total,
	--Обороты за отчетный период по кредиту в рублях
	sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.credit_amount_rub
			else 0
		end
	) 										as turn_cre_rub,
	--Обороты за отчетный период по кредиту в валюте, переведенный в рубли
	sum(
		case
			when cur.currency_code not in ('643', '810')
				then at.credit_amount_rub * ech_r.reduced_cource
			else 0
		end
		) 									as turn_cre_val,
	--Итого: Обороты за отчетный период по кредиту в рублях + Обороты за отчетный период по кредиту в валюте, переведенный в рубли 
	 sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.credit_amount_rub
			else at.credit_amount_rub * ech_r.reduced_cource
		end
		) 									as turn_cre_total
```
