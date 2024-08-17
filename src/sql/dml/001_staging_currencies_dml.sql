MERGE INTO
    STV2024071554__STAGING.currencies tgt
USING
    STV2024071554__STAGING.currencies_inc src
ON
    tgt.date_update = src.date_update
    and tgt.currency_code = src.currency_code
    and tgt.currency_code_with = src.currency_code_with
WHEN MATCHED and (
    src.currency_with_div <> tgt.currency_with_div
)
    THEN UPDATE SET
                currency_with_div = src.currency_with_div
WHEN NOT MATCHED
    THEN INSERT (
                currency_code,
                currency_code_with,
                date_update,
                currency_with_div
        )
    VALUES (
                src.currency_code,
                src.currency_code_with,
                src.date_update,
                src.currency_with_div
        )
;