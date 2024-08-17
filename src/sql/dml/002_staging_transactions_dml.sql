MERGE INTO
    STV2024071554__STAGING.transactions tgt
USING
    STV2024071554__STAGING.transactions_inc src
ON 
    tgt.operation_id = src.operation_id
    and tgt.transaction_dt = src.transaction_dt
WHEN MATCHED and (
    src.account_number_from <> tgt.account_number_from
    or src.account_number_to <> tgt.account_number_to
    or src.currency_code <> tgt.currency_code
    or src.country <> tgt.country
    or src."status" <> tgt."status"
    or src.transaction_type <> tgt.transaction_type
    or src.amount <> tgt.amount
)
    THEN UPDATE SET
                account_number_from = src.account_number_from,
                account_number_to = src.account_number_to,
                currency_code = src.currency_code,
                country = src.country,
                "status" = src."status",
                transaction_type = src.transaction_type,
                amount = src.amount
WHEN NOT MATCHED
    THEN INSERT (
                operation_id,
                account_number_from,
                account_number_to,
                currency_code,
                country,
                "status",
                transaction_type,
                amount,
                transaction_dt
        )
    VALUES (
                src.operation_id,
                src.account_number_from,
                src.account_number_to,
                src.currency_code,
                src.country,
                src."status",
                src.transaction_type,
                src.amount,
                src.transaction_dt
        )
;
