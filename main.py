#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import pandas
import jaydebeapi

# Подключение к базе Oracle

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'],
'/home/de1m/ojdbc8.jar'
)
curs = conn.cursor()
conn.jconn.setAutoCommit(False)

# В последний день понял что назвал таблицы не совсем так как в ТЗ, сорри

curs.execute( "truncate table de1m.kmlv_stg_cards" )
curs.execute( "truncate table de1m.kmlv_stg_cards_del" )
curs.execute( "truncate table de1m.kmlv_stg_accounts" )
curs.execute( "truncate table de1m.kmlv_stg_accounts_del" )
curs.execute( "truncate table de1m.kmlv_stg_clients" )
curs.execute( "truncate table de1m.kmlv_stg_clients_del" )
curs.execute( "truncate table de1m.kmlv_dwh_fact_pssprt_blcklst" )
curs.execute( "truncate table de1m.kmlv_meta_data" )
curs.execute( "truncate table de1m.kmlv_stg_terminals" )
curs.execute( "truncate table de1m.kmlv_stg_terminals_del" )

# Захват из трёх таблиц схемы BANK в Staging (Extract)

curs.execute( """ insert into de1m.kmlv_stg_cards ( CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT )
select
    CARD_NUM,
    ACCOUNT,
    CREATE_DT,
    UPDATE_DT
from bank.cards
where coalesce( UPDATE_DT, CREATE_DT ) > coalesce( ( 
    select MAX_UPDATE_DT
    from de1m.kmlv_meta_cards
    where schema_name = 'DE1M' and table_name = 'KMLV_CARDS'
), to_date( '01.01.1800 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )) """ )

curs.execute( """ insert into de1m.kmlv_stg_cards_del ( ACCOUNT )
select ACCOUNT from bank.cards """ )

curs.execute( """ insert into de1m.kmlv_stg_accounts ( ACCOUNT, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT )
select
    ACCOUNT,
    VALID_TO,
    CLIENT,
    CREATE_DT,
    UPDATE_DT
from bank.accounts
where coalesce( UPDATE_DT, CREATE_DT ) > coalesce( ( 
    select MAX_UPDATE_DT
    from de1m.kmlv_meta_accounts
    where schema_name = 'DE1M' and table_name = 'KMLV_ACCOUNT'
), to_date( '01.01.1800 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )) """ )

curs.execute( """ insert into de1m.kmlv_stg_accounts_del ( ACCOUNT )
select ACCOUNT from bank.accounts """ )

curs.execute( """ insert into de1m.kmlv_stg_clients ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT )
select
    CLIENT_ID,
    LAST_NAME,
    FIRST_NAME,
    PATRONYMIC,
    DATE_OF_BIRTH,
    PASSPORT_NUM,
    PASSPORT_VALID_TO,
    PHONE,
    CREATE_DT,
    UPDATE_DT
from bank.clients
where coalesce( UPDATE_DT, CREATE_DT ) > coalesce( ( 
    select MAX_UPDATE_DT
    from de1m.kmlv_meta_clients
    where schema_name = 'DE1M' and table_name = 'KMLV_CLIENTS'
), to_date( '01.01.1800 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )) """ )

curs.execute( """ insert into de1m.kmlv_stg_clients_del ( CLIENT_ID )
select CLIENT_ID from bank.clients """ )


# Поиск имен файлов в рабочем каталоге

spisok = os.listdir(r"/home/de1m/kmlv")
for s in spisok:
    if s.find('transactions_') > -1: trans = s
    elif s.find('passport_blacklist_') > -1: black = s
    elif s.find('terminals_') > -1: term = s

# Загрузка из файлов в целевые таблицы и переименование

df = pandas.read_csv( trans, sep=';', header=0, index_col=None )
df = df.astype(str)
curs.executemany( "insert into de1m.kmlv_dwh_fact_transactions( transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal ) values ( ?, to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), ?, ?, ?, ?, ? )", df.values.tolist() )

curs.execute( """ insert into de1m.kmlv_meta_data ( data_trans ) values ( ( select max( transaction_date ) from de1m.kmlv_dwh_fact_transactions ) ) """ )

os.rename( trans, '/home/de1m/kmlv/archive/' + trans + '.backup' )

df = pandas.read_excel( black, sheet_name='blacklist', header=0, index_col=None )
df = df.astype(str)
curs.executemany( "insert into de1m.kmlv_dwh_fact_pssprt_blcklst( data, passport ) values ( to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), ? )", df.values.tolist() )

os.rename( black, '/home/de1m/kmlv/archive/' + black + '.backup' )

df = pandas.read_excel( term, sheet_name='terminals', header=0, index_col=None )
df = df.astype(str)
curs.executemany( "insert into de1m.kmlv_stg_terminals( terminal_id, terminal_type, terminal_city, terminal_address ) values ( ?, ?, ?, ? )", df.values.tolist() )

os.rename( term, '/home/de1m/kmlv/archive/' + term + '.backup' )

curs.execute( """ insert into de1m.kmlv_stg_terminals_del ( terminal_id )
select terminal_id from de1m.kmlv_stg_terminals """ )


# Выделение вставок и изменений (transform), вставка их в приемник (load)

curs.execute( """ merge into de1m.kmlv_dwh_dim_cards_hist tgt
using de1m.kmlv_stg_cards stg
on( stg.ACCOUNT = tgt.ACCOUNT and deleted_flg = 'N' )
when matched then 
    update set tgt.EFFECTIVE_TO = stg.UPDATE_DT - interval '1' second
    where 1=1
	and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
	and (1=0
    or stg.CARD_NUM <> tgt.CARD_NUM or ( stg.CARD_NUM is null and tgt.CARD_NUM is not null ) or ( stg.CARD_NUM is not null and tgt.CARD_NUM is null )
	)
when not matched then 
    insert ( CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
    values ( stg.CARD_NUM, stg.ACCOUNT, stg.UPDATE_DT, to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 'N' ) """ )

curs.execute( """ insert into de1m.kmlv_dwh_dim_cards_hist ( CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    stg.CARD_NUM,
    stg.ACCOUNT,
    stg.CREATE_DT,
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 
    'N'
from de1m.kmlv_dwh_dim_cards_hist tgt
inner join de1m.kmlv_stg_cards stg
on ( stg.ACCOUNT = tgt.ACCOUNT and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) and deleted_flg = 'N' )
where 1=0
    or stg.CARD_NUM <> tgt.CARD_NUM or ( stg.CARD_NUM is null and tgt.CARD_NUM is not null ) or ( stg.CARD_NUM is not null and tgt.CARD_NUM is null ) """ )

curs.execute( """ merge into de1m.kmlv_dwh_dim_accounts_hist tgt
using de1m.kmlv_stg_accounts stg
on( stg.ACCOUNT = tgt.ACCOUNT and deleted_flg = 'N' )
when matched then 
    update set tgt.EFFECTIVE_TO = stg.UPDATE_DT - interval '1' second
    where 1=1
    and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
    and (1=0
    or stg.CLIENT <> tgt.CLIENT or ( stg.CLIENT is null and tgt.CLIENT is not null ) or ( stg.CLIENT is not null and tgt.CLIENT is null )
    )
when not matched then
    insert ( ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
    values ( stg.ACCOUNT, stg.VALID_TO, stg.CLIENT, stg.UPDATE_DT, to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 'N' ) """ )

curs.execute( """ insert into de1m.kmlv_dwh_dim_accounts_hist ( ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    stg.ACCOUNT,
    stg.VALID_TO,
    stg.CLIENT,
    stg.CREATE_DT,
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 
    'N'
from de1m.kmlv_dwh_dim_accounts_hist tgt
inner join de1m.kmlv_stg_accounts stg
on ( stg.ACCOUNT = tgt.ACCOUNT and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) and deleted_flg = 'N' )
where 1=0
    or stg.CLIENT <> tgt.CLIENT or ( stg.CLIENT is null and tgt.CLIENT is not null ) or ( stg.CLIENT is not null and tgt.CLIENT is null ) """ )

curs.execute( """ merge into de1m.kmlv_dwh_dim_clients_hist tgt
using de1m.kmlv_stg_clients stg
on( stg.CLIENT_ID = tgt.CLIENT_ID and deleted_flg = 'N' )
when matched then 
    update set tgt.EFFECTIVE_TO = stg.UPDATE_DT - interval '1' second
    where 1=1
    and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
    and (1=0
    or stg.CLIENT_ID <> tgt.CLIENT_ID or ( stg.CLIENT_ID is null and tgt.CLIENT_ID is not null ) or ( stg.CLIENT_ID is not null and tgt.CLIENT_ID is null )
    )
when not matched then 
    insert ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
    values ( stg.CLIENT_ID, stg.LAST_NAME, stg.FIRST_NAME, stg.PATRONYMIC, stg.DATE_OF_BIRTH, stg.PASSPORT_NUM, stg.PASSPORT_VALID_TO, stg.PHONE, stg.UPDATE_DT, to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 'N' ) """ )

curs.execute( """ insert into de1m.kmlv_dwh_dim_clients_hist ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    stg.CLIENT_ID,
    stg.LAST_NAME,
    stg.FIRST_NAME,
    stg.PATRONYMIC,
    stg.DATE_OF_BIRTH,
    stg.PASSPORT_NUM,
    stg.PASSPORT_VALID_TO,
    stg.PHONE,
    stg.CREATE_DT,
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 
    'N'
from de1m.kmlv_dwh_dim_clients_hist tgt
inner join de1m.kmlv_stg_clients stg
on ( stg.CLIENT_ID = tgt.CLIENT_ID and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) and deleted_flg = 'N' )
where 1=0
    or stg.CLIENT_ID <> tgt.CLIENT_ID or ( stg.CLIENT_ID is null and tgt.CLIENT_ID is not null ) or ( stg.CLIENT_ID is not null and tgt.CLIENT_ID is null ) """ )

curs.execute( """ merge into de1m.kmlv_meta trg
using ( select 'DE1M' schema_name, 'KMLV_META' table_name, to_date((select data_trans from de1m.kmlv_meta_data), 'dd.mm.yyyy') as MAX_UPDATE_DT from dual) src
on (trg.schema_name = src.schema_name and trg.table_name = src.table_name)
when matched then 
    update set trg.MAX_UPDATE_DT = src.MAX_UPDATE_DT
    where src.MAX_UPDATE_DT is not null
when not matched then
    insert ( schema_name, table_name, MAX_UPDATE_DT )
    values ( 'DE1M','KMLV_META', to_date((select data_trans from de1m.kmlv_meta_data), 'DD.MM.YYYY HH24:MI:SS') ) """ )

curs.execute( """ merge into de1m.kmlv_dwh_dim_terminals_hist tgt
using de1m.kmlv_stg_terminals stg
on( stg.terminal_id = tgt.terminal_id )
when matched then
    update set tgt.EFFECTIVE_TO = (to_date((select data_trans from de1m.kmlv_meta_data), 'DD.MM.YYYY')) - interval '1' second, deleted_flg = 'N'
    where tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
    and (1=0
    or ( stg.terminal_type <> tgt.terminal_type
        or ( stg.terminal_type is null and tgt.terminal_type is not null )
        or ( stg.terminal_type is not null and tgt.terminal_type is null )
    or stg.terminal_city <> tgt.terminal_city
        or ( stg.terminal_city is null and tgt.terminal_city is not null )
        or ( stg.terminal_city is not null and tgt.terminal_city is null )
    or stg.terminal_address <> tgt.terminal_address
        or ( stg.terminal_address is null and tgt.terminal_address is not null )
        or ( stg.terminal_address is not null and tgt.terminal_address is null )))
when not matched then 
    insert ( terminal_id, terminal_type, terminal_city, terminal_address, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
    values ( stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city,
            stg.terminal_address,
            COALESCE(( select max_update_dt from de1m.kmlv_meta where schema_name = 'DE1M' and table_name = 'KMLV_META'),
            to_date( '01.01.1800 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )),
            to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ),
            'N' ) """ )


curs.execute( """ insert into de1m.kmlv_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    to_date((select data_trans from de1m.kmlv_meta_data), 'DD.MM.YYYY HH24:MI:SS'),
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ),
    'N'
from de1m.kmlv_dwh_dim_terminals_hist tgt
left join de1m.kmlv_stg_terminals stg
on tgt.terminal_id = stg.terminal_id 
where stg.terminal_id is not NULL 
and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
and (1=0
    or (stg.terminal_type <> tgt.terminal_type
        or ( stg.terminal_type is null and tgt.terminal_type is not null )
        or ( stg.terminal_type is not null and tgt.terminal_type is null )
    or stg.terminal_city <> tgt.terminal_city
        or ( stg.terminal_city is null and tgt.terminal_city is not null )
        or ( stg.terminal_city is not null and tgt.terminal_city is null )
    or stg.terminal_address <> tgt.terminal_address
        or ( stg.terminal_address is null and tgt.terminal_address is not null )
        or ( stg.terminal_address is not null and tgt.terminal_address is null ))) """ )


# Обработка удалений

curs.execute( """ insert into de1m.kmlv_dwh_dim_cards_hist ( ACCOUNT, CARD_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    tgt.ACCOUNT,
    tgt.CARD_NUM,
    current_date,
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 
    'Y'
from de1m.kmlv_dwh_dim_cards_hist tgt
left join de1m.kmlv_stg_cards_del stg
on ( stg.ACCOUNT = tgt.ACCOUNT and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) and deleted_flg = 'N' )
where stg.ACCOUNT is null """ )

curs.execute( """ update de1m.kmlv_dwh_dim_cards_hist tgt
set EFFECTIVE_TO = current_date - interval '1' second
where tgt.ACCOUNT not in (select ACCOUNT from de1m.kmlv_stg_cards)
and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
and tgt.deleted_flg = 'N' """ )

curs.execute( """ insert into de1m.kmlv_dwh_dim_accounts_hist ( ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    tgt.ACCOUNT,
    tgt.VALID_TO,
    tgt.CLIENT,
    current_date,
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 
    'Y'
from de1m.kmlv_dwh_dim_accounts_hist tgt
left join de1m.kmlv_stg_accounts_del stg
on ( stg.ACCOUNT = tgt.ACCOUNT and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) and deleted_flg = 'N' )
where stg.ACCOUNT is null """ )

curs.execute( """ update de1m.kmlv_dwh_dim_accounts_hist tgt
set EFFECTIVE_TO = current_date - interval '1' second
where tgt.ACCOUNT not in (select ACCOUNT from de1m.kmlv_stg_accounts)
and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
and tgt.deleted_flg = 'N' """ )

curs.execute( """ insert into de1m.kmlv_dwh_dim_clients_hist ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select
    tgt.CLIENT_ID,
    tgt.LAST_NAME,
    tgt.FIRST_NAME,
    tgt.PATRONYMIC,
    tgt.DATE_OF_BIRTH,
    tgt.PASSPORT_NUM,
    tgt.PASSPORT_VALID_TO,
    tgt.PHONE,
    current_date,
    to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ), 
    'Y'
from de1m.kmlv_dwh_dim_clients_hist tgt
left join de1m.kmlv_stg_clients_del stg
on ( stg.CLIENT_ID = tgt.CLIENT_ID and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) and deleted_flg = 'N' )
where stg.CLIENT_ID is null """ )

curs.execute( """ update de1m.kmlv_dwh_dim_clients_hist tgt
set EFFECTIVE_TO = current_date - interval '1' second
where tgt.CLIENT_ID not in (select CLIENT_ID from de1m.kmlv_stg_clients)
and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' )
and tgt.deleted_flg = 'N' """ )

curs.execute( """ insert into de1m.kmlv_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, EFFECTIVE_FROM, EFFECTIVE_TO, deleted_flg  ) 
select tgt.terminal_id,
       tgt.terminal_type,
       tgt.terminal_city,
       tgt.terminal_address,
       current_date,
       to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ),
       'Y'
from de1m.kmlv_dwh_dim_terminals_hist tgt
left join de1m.kmlv_stg_terminals_del stg
on tgt.terminal_id = stg.terminal_id
where stg.terminal_id is null 
and tgt.EFFECTIVE_TO = to_date( '31.12.9999 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) 
and tgt.deleted_flg <> 'Y' """ )

# Обновление метаданных

curs.execute( """ merge into de1m.kmlv_meta_cards trg
using ( select 'DE1M' schema_name, 'KMLV_CARDS' table_name, ( select max( CREATE_DT ) from de1m.kmlv_stg_cards ) MAX_UPDATE_DT from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.MAX_UPDATE_DT = src.MAX_UPDATE_DT
    where src.MAX_UPDATE_DT is not null
when not matched then 
    insert ( schema_name, table_name, MAX_UPDATE_DT )
    values ( 'DE1M', 'KMLV_CARDS', coalesce( src.MAX_UPDATE_DT, to_date( '01.01.1900 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) ) ) """ )

curs.execute( """ merge into de1m.kmlv_meta_accounts trg
using ( select 'DE1M' schema_name, 'KMLV_ACCOUNT' table_name, ( select max( CREATE_DT ) from de1m.kmlv_stg_accounts ) MAX_UPDATE_DT from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.MAX_UPDATE_DT = src.MAX_UPDATE_DT
    where src.MAX_UPDATE_DT is not null
when not matched then 
    insert ( schema_name, table_name, MAX_UPDATE_DT )
    values ( 'DE1M', 'KMLV_ACCOUNT', coalesce( src.MAX_UPDATE_DT, to_date( '01.01.1900 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) ) ) """ )

curs.execute( """ merge into de1m.kmlv_meta_clients trg
using ( select 'DE1M' schema_name, 'KMLV_CLIENTS' table_name, ( select max( CREATE_DT ) from de1m.kmlv_stg_clients ) MAX_UPDATE_DT from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.MAX_UPDATE_DT = src.MAX_UPDATE_DT
    where src.MAX_UPDATE_DT is not null
when not matched then 
    insert ( schema_name, table_name, MAX_UPDATE_DT )
    values ( 'DE1M', 'KMLV_CLIENTS', coalesce( src.MAX_UPDATE_DT, to_date( '01.01.1900 00:00:00', 'DD.MM.YYYY HH24:MI:SS' ) ) ) """ )


# 1. Совершение операций при просроченном или заблокированном паспорте

curs.execute( """ INSERT INTO de1m.kmlv_rep_fraud
( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT )
SELECT
    trns.TRANSACTION_DATE,
    clnt.PASSPORT_NUM,
    clnt.LAST_NAME||' '||clnt.FIRST_NAME||' '||clnt.PATRONYMIC,
    clnt.PHONE,
    '1',
    CURRENT_DATE
FROM de1m.kmlv_dwh_fact_transactions trns
    LEFT JOIN de1m.kmlv_dwh_dim_cards_hist crds ON trns.CARD_NUM = rtrim(crds.CARD_NUM)
    LEFT JOIN de1m.kmlv_dwh_dim_accounts_hist acnt ON crds.ACCOUNT  = acnt.ACCOUNT
    LEFT JOIN de1m.kmlv_dwh_dim_clients_hist clnt ON acnt.CLIENT  = clnt.CLIENT_ID
    LEFT JOIN de1m.kmlv_dwh_fact_pssprt_blcklst pblk ON clnt.PASSPORT_NUM  = pblk.PASSPORT
WHERE trns.TRANSACTION_DATE > clnt.PASSPORT_VALID_TO OR clnt.PASSPORT_NUM = pblk.PASSPORT """ )

# 2. Совершение операций при недействующем договоре

curs.execute( """ INSERT INTO de1m.kmlv_rep_fraud
( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT )
SELECT
    trns.TRANSACTION_DATE,
    clnt.PASSPORT_NUM,
    clnt.LAST_NAME||' '||clnt.FIRST_NAME||' '||clnt.PATRONYMIC,
    clnt.PHONE,
    '2',
    CURRENT_DATE
FROM de1m.kmlv_dwh_fact_transactions trns
    LEFT JOIN de1m.kmlv_dwh_dim_cards_hist crds ON trns.CARD_NUM = rtrim(crds.CARD_NUM)
    LEFT JOIN de1m.kmlv_dwh_dim_accounts_hist acnt ON crds.ACCOUNT  = acnt.ACCOUNT
    LEFT JOIN de1m.kmlv_dwh_dim_clients_hist clnt ON acnt.CLIENT  = clnt.CLIENT_ID
WHERE trns.TRANSACTION_DATE > acnt.VALID_TO """ )


conn.commit()

curs.close()
conn.close()
