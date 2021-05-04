CREATE OR REPLACE VIEW gosipenkov.ods_v_payment AS
SELECT
    -- payload
    user_id,
    doc_type,
    doc_num,
    account,
    phone,
    billing_period,
    pay_date,
    amount,

    -- calculated
    load_date,
    'gosipenkov.stg_payment' record_source,
    pay_date AS effective_from,

    -- <https://dbtvault.readthedocs.io/en/latest/best_practices/#how-do-we-hash>

    CAST(MD5_BINARY(NULLIF(CONCAT_WS('||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_type AS VARCHAR))), ''), '^^'),
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_num AS VARCHAR))), ''), '^^')
    ), '^^||^^')) AS BINARY(16)) AS pay_doc_pk,

    CAST(MD5_BINARY(NULLIF(CONCAT_WS('||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^')
    ), '^^')) AS BINARY(16)) AS user_pk,

    CAST(MD5_BINARY(NULLIF(CONCAT_WS('||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_type AS VARCHAR))), ''), '^^'),
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_num AS VARCHAR))), ''), '^^'),
        IFNULL(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^')
    ), '^^||^^||^^')) AS BINARY(16)) AS payment_pk,

    CAST(MD5_BINARY(CONCAT_WS('||',
        -- hub PK fields
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_type AS VARCHAR))), ''), '^^'),
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_num AS VARCHAR))), ''), '^^'),
        -- payload fields
        IFNULL(NULLIF(UPPER(TRIM(CAST(pay_date AS VARCHAR))), ''), '^^'),
        IFNULL(NULLIF(UPPER(TRIM(CAST(amount AS VARCHAR))), ''), '^^')
    )) AS AS BINARY(16)) AS pay_doc_detail_hashdiff,

    CAST(MD5_BINARY(CONCAT_WS('||',
        -- hub PK fields
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_type AS VARCHAR))), ''), '^^')
        IFNULL(NULLIF(UPPER(TRIM(CAST(doc_num AS VARCHAR))), ''), '^^')
        IFNULL(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^')
        -- payload fields
        IFNULL(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''), '^^')
    )) AS AS BINARY(16)) AS payment_billing_period_hashdiff

    CAST(MD5_BINARY(CONCAT_WS('||',
        -- hub PK fields
        IFNULL(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^')
        -- payload fields
        IFNULL(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''), '^^'),
        IFNULL(NULLIF(UPPER(TRIM(CAST(phone AS VARCHAR))), ''), '^^')
    )) AS AS BINARY(16)) AS user_detail_hashdiff
FROM
    gosipenkov.ods_payment;
