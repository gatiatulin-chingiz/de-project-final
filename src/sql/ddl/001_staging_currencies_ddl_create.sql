drop table if exists STV2024071554__STAGING.currencies;
create table if not exists STV2024071554__STAGING.currencies (
    currency_code integer not null,
    currency_code_with integer not null,
    date_update date not null,
    currency_with_div numeric(5,3) not null,
    primary key (date_update, currency_code, currency_code_with)
)
order by date_update
segmented by hash(date_update) all nodes
;

drop table if exists STV2024071554__STAGING.currencies_inc;
CREATE TABLE if not exists STV2024071554__STAGING.currencies_inc LIKE STV2024071554__STAGING.currencies INCLUDING PROJECTIONS;