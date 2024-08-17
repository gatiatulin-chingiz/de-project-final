TRUNCATE TABLE STV2024071554__DWH.global_metrics_inc;

INSERT INTO STV2024071554__DWH.global_metrics_inc (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
with currency_rate as (
    select
        currency_code,
        currency_code_with,
        currency_with_div
    from STV2024071554__STAGING.currencies
    where 1=1
        and date_update = '{calc_date}'
        and currency_code = 420
),
transactions_cut as (
    select
        transaction_dt::date as date_update,
        currency_code as currency_from,
        account_number_from,
        amount,
        count(*) over(partition by currency_code) as total_currency_transactions
    from STV2024071554__STAGING.transactions
    where 1=1
        and transaction_dt::date = '{calc_date}'
        and account_number_from > 0
        and account_number_to > 0
        and status = 'done'
),
t_1 as (
    select
        sst.date_update,
        sst.currency_from,
        sst.account_number_from,
        sum(sst.amount) as amount_sum,
        count(sst.amount) as cnt_transactions,
        sst.total_currency_transactions
    from transactions_cut sst
    group by sst.date_update, sst.currency_from, sst.account_number_from, sst.total_currency_transactions
),
t_2 as (
    select
        date_update,
        currency_from,
        sum(amount_sum) as amount_local_currency_total,
        total_currency_transactions as cnt_transactions,
        avg(cnt_transactions) as avg_transactions_per_account,
        count(*) as cnt_accounts_make_transactions
    from t_1
    group by date_update, currency_from, total_currency_transactions
)
select
    t_2.date_update,
    t_2.currency_from,
    case
        when cr.currency_with_div is null then amount_local_currency_total
        else cr.currency_with_div * amount_local_currency_total
    end as amount_total,
    t_2.cnt_transactions,
    t_2.avg_transactions_per_account,
    t_2.cnt_accounts_make_transactions
from t_2
left join currency_rate cr
    on cr.currency_code_with = t_2.currency_from;

MERGE INTO
    STV2024071554__DWH.global_metrics tgt
USING
    STV2024071554__DWH.global_metrics_inc src
ON
    tgt.date_update = src.date_update
    and tgt.currency_from = src.currency_from
WHEN MATCHED and (
    src.amount_total <> tgt.amount_total
    or src.cnt_transactions <> tgt.cnt_transactions
    or src.avg_transactions_per_account <> tgt.avg_transactions_per_account
    or src.cnt_accounts_make_transactions <> tgt.cnt_accounts_make_transactions
)
    THEN UPDATE SET
                amount_total = src.amount_total,
                cnt_transactions = src.cnt_transactions,
                avg_transactions_per_account = src.avg_transactions_per_account,
                cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED
    THEN INSERT (
                date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions
        )
    VALUES (
                src.date_update,
                src.currency_from,
                src.amount_total,
                src.cnt_transactions,
                src.avg_transactions_per_account,
                src.cnt_accounts_make_transactions
        )
;
