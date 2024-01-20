/*
 * Создание схемы dm(data mart - витрина данных)
 */

create schema dm;

/*
 * Создадим таблицу dm.dm_account_turnover_f.
 */

create table dm.dm_account_turnover_f (
	on_date				date,
    account_rk 			numeric,
    credit_amount 		numeric(23, 8),
    credit_amount_rub 	numeric(23, 8),
    debet_amount		numeric(23, 8),
    debet_amount_rub	numeric(23, 8)
);


/*
 * Прототип для таблицы dm.dm_account_turnover_f, 
 * рассчитывающий кредитовые и дебетовые обороты по счетам за каждый день января 2018 года.
 */

insert into dm.dm_account_turnover_f (
	on_date,
	account_rk,
	credit_amount,
	credit_amount_rub,
	debet_amount,
	debet_amount_rub
)
with wt_turn as 
(
	select
		date(p.oper_date)    				as oper_date,
		er.data_actual_date 				as exchange_rate,
		er.reduced_cource 					as reduced_cource,
		p.credit_account_rk 				as account_rk,
		p.credit_amount 					as credit_amount,
		p.credit_amount * er.reduced_cource as credit_amount_rub,
		cast(null as numeric) 				as debet_amount,
		cast(null as numeric)				as debet_amount_rub
	from ds.ft_posting_f p
	join ds.md_account_d a on p.credit_account_rk = a.account_rk
	left join ds.md_exchange_rate_d er 
		on a.currency_rk = er.currency_rk
		and date(p.oper_date) >= '2018-01-01' and date(oper_date) < '2018-02-01'
	where date(p.oper_date) = er.data_actual_date
	union all
	select
		date(p.oper_date)    				as oper_date,
		er.data_actual_date 				as exchange_rate,
		er.reduced_cource 					as reduced_cource,
		p.credit_account_rk 				as account_rk,
		cast(null as numeric) 				as credit_amount,
		cast(null as numeric)				as credit_amount_rub,
		p.debet_amount 						as debet_amount,
		p.debet_amount * er.reduced_cource 	as debet_amount_rub
	from ds.ft_posting_f p
	join ds.md_account_d a on p.credit_account_rk = a.account_rk
	left join ds.md_exchange_rate_d er 
		on a.currency_rk = er.currency_rk
		and date(p.oper_date) >= '2018-01-01' and date(oper_date) < '2018-02-01'
	where date(p.oper_date) = er.data_actual_date
)
select
	t.oper_date								as on_date,
	t.account_rk 							as account_rk,
	sum(t.credit_amount) 					as credit_amount,
	sum(t.credit_amount_rub) 				as credit_amount_rub,
	sum(t.debet_amount) 					as debet_amount,
	sum(t.debet_amount_rub) 				as debet_amount_rub
from wt_turn t
group by
	t.oper_date,
	t.account_rk;
	

--truncate dm.dm_account_turnover_f;
select * from dm.dm_account_turnover_f order by on_date, account_rk;


/*
 * Создадим таблицу dm.dm_f101_round_f.
 */

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

/* Прототип для таблицы dm.dm_f101_round_f, рассчитывающий:
 * 1. «Входящие остатки (в рублях/валют/итого)" - рассчитать как баланс по счетам на отчетную дату.
 * 2. «Обороты за отчетный период по дебету/кредиту (в рублях/валют/итого)» - рассчитать как сумму всех проводок за отчетную дату по дебету/кредиту соответственно.
 * 3. «Исходящие остатки (в рублях)»:
 * 	а. Для счетов с признаком "Актив" и currency_code '643' рассчитать как Входящие остатки (в руб.) - Обороты по кредиту (в руб.) + Обороты по дебету (в руб.).
 *  б. Для счетов с признаком "Актив" и currency_code '810' рассчитать как Входящие остатки (в руб.) - Обороты по кредиту (в руб.) + Обороты по дебету (в руб.).
 * 	в. Для счетов с признаком "Пассив" и currency_code '643' рассчитать как Входящие остатки (в руб.) + Обороты по кредиту (в руб.) - Обороты по дебету (в руб.).
 *	г. Для счетов с признаком "Пассив" и currency_code '810' рассчитать как Входящие остатки (в руб.) + Обороты по кредиту (в руб.) - Обороты по дебету (в руб.).
 * 4. «Исходящие остатки (в валюте)»: 
 *	а. Для счетов с признаком "Актив" и currency_code не '643' и не '810' рассчитать как Входящие остатки (в валюте) - Обороты по кредиту (в валюте) + Обороты по дебету (в валюте).
 *	б. Для счетов с признаком "Пассив" и currency_code не '643' и не '810' рассчитать Входящие остатки (в валюте) + Обороты по кредиту (в валюте) - Обороты по дебету в валюте.
 * 5. «Исходящие остатки (итого)» - рассчитать как Исходящие остатки (в валюте) + Исходящие остатки (в рублях).  
 */

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

select
	at.on_date 								as rep_date,
	s.chapter 								as chapter,
	substring(acc_d.account_number, 1, 5) 	as ledger_account,
	acc_d.char_type 						as characteristic,
	--RUB balance
	sum(
		case
			when cur.currency_code in ('643', '810')
				then b.balance_out
			else 0
		end
		) 									as bal_in_rub,
	--VAL balance conver to rub
	sum(
		case
			when cur.currency_code not in ('643', '810')
				then b.balance_out * ech_r.reduced_cource
			else 0
		end
		) 									as bal_in_val,
	--Total: RUB balance + VAL balance conver to rub
	sum(
		case 
			when cur.currency_code in ('643', '810')
				then b.balance_out
			else b.balance_out * ech_r.reduced_cource
		end
		) 									as bal_in_total,
	--RUB debet turnover
	sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.debet_amount_rub
			else 0
		end
	) 										as turn_deb_rub,
	--VAL debet turnover conver to rub
	sum(
		case
			when cur.currency_code not in ('643', '810')
				then at.debet_amount_rub * ech_r.reduced_cource
			else 0
		end
		) 									as turn_deb_val,
	--Total: RUB debet turnover + VAL debet turnover conver to rub
	 sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.debet_amount_rub
			else at.debet_amount_rub * ech_r.reduced_cource
		end
		) 									as turn_deb_total,
	--RUB credit turnover
	sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.credit_amount_rub
			else 0
		end
	) 										as turn_cre_rub,
	--VAL credit turnover conver to rub
	sum(
		case
			when cur.currency_code not in ('643', '810')
				then at.credit_amount_rub * ech_r.reduced_cource
			else 0
		end
		) 									as turn_cre_val,
	--Total: RUB credit turnover + VAL credit turnover conver to rub
	 sum(
		case 
			when cur.currency_code in ('643', '810')
				then at.credit_amount_rub
			else at.credit_amount_rub * ech_r.reduced_cource
		end
		) 									as turn_cre_total,
	--Balance out RUB
	sum(
		case
			when cur.currency_code in ('643', '840') and s.characteristic = 'А'
				then b.balance_out - at.credit_amount_rub + at.debet_amount_rub
			when cur.currency_code in ('643', '840') and s.characteristic = 'П'
				then b.balance_out + at.credit_amount_rub - at.debet_amount_rub
			else 0
		end
	) 										as bal_out_rub,
	--Balance out VAL
	sum(
		case
			when cur.currency_code not in ('643', '840') and s.characteristic = 'А'
				then b.balance_out * ech_r.reduced_cource - at.credit_amount_rub * ech_r.reduced_cource + at.debet_amount_rub * ech_r.reduced_cource
			when cur.currency_code in ('643', '840') and s.characteristic = 'П'
				then b.balance_out * ech_r.reduced_cource + at.credit_amount_rub * ech_r.reduced_cource - at.debet_amount_rub * ech_r.reduced_cource
			else 0
		end
	) 										as bal_out_val,
	--Balance out total
	sum(
		case
			when cur.currency_code in ('643', '840') and s.characteristic = 'А'
				then b.balance_out - at.credit_amount_rub + at.debet_amount_rub
			when cur.currency_code in ('643', '840') and s.characteristic = 'П'
				then b.balance_out + at.credit_amount_rub - at.debet_amount_rub
			else 0
		end
	) + sum(
		case
			when cur.currency_code not in ('643', '840') and s.characteristic = 'А'
				then b.balance_out * ech_r.reduced_cource - at.credit_amount_rub * ech_r.reduced_cource + at.debet_amount_rub * ech_r.reduced_cource
			when cur.currency_code in ('643', '840') and s.characteristic = 'П'
				then b.balance_out * ech_r.reduced_cource + at.credit_amount_rub * ech_r.reduced_cource - at.debet_amount_rub * ech_r.reduced_cource
			else 0
		end
	) 										as bal_out_total
from ds.md_ledger_account_s s
join ds.md_account_d acc_d 
	on s.ledger_account = substring(acc_d.account_number, 1, 5)::int
join ds.md_currency_d cur
	on acc_d.currency_rk = cur.currency_rk 
join ds.ft_balance_f b 
	on acc_d.account_rk = b.account_rk
left join ds.md_exchange_rate_d ech_r 
	on acc_d.currency_rk = ech_r.currency_rk
left join dm.dm_account_turnover_f at
	on acc_d.account_rk = at.account_rk
group by 
	at.on_date,
	s.chapter,
	acc_d.account_number,
	acc_d.char_type;


--truncate dm.dm_f101_round_f;
select * from dm.dm_f101_round_f;












