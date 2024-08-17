drop table if exists STV2024071554__STAGING.transactions;
create table if not exists STV2024071554__STAGING.transactions (
    operation_id UUID not null,
    account_number_from integer not null,
    account_number_to integer not null,
    currency_code integer not null,
    country varchar(50) not null,
    "status" varchar(15) not null,
    transaction_type varchar(50) not null,
    amount integer not null,
    transaction_dt timestamp(3) not null,
    primary key (operation_id)
)
order by transaction_dt
segmented by hash(transaction_dt, operation_id) all nodes
;

drop table if exists STV2024071554__STAGING.transactions_inc;
CREATE TABLE if not exists STV2024071554__STAGING.transactions_inc LIKE STV2024071554__STAGING.transactions INCLUDING PROJECTIONS;
