drop table if exists STV2024071554__DWH.global_metrics;
create table if not exists STV2024071554__DWH.global_metrics (
    date_update date not null,
    currency_from integer not null,
    amount_total integer not null,
    cnt_transactions integer not null,
    avg_transactions_per_account integer not null,
    cnt_accounts_make_transactions integer not null,
    primary key (date_update, currency_from)
)
order by date_update, currency_from
segmented by hash(date_update, currency_from) all nodes
;

drop table if exists STV2024071554__DWH.global_metrics_inc;
CREATE TABLE if not exists STV2024071554__DWH.global_metrics_inc LIKE STV2024071554__DWH.global_metrics INCLUDING PROJECTIONS;
