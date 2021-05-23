CREATE OR REPLACE VIEW gosipenkov.ods_v_user AS
SELECT
    -- payload
    user_id,
    legal_type,
    district,
    reg_date,
    billing_mode,
    is_vip,

    -- calculated
    load_date,
    'gosipenkov.stg_user' record_source,
    load_date AS effective_from,

    -- <https://dbtvault.readthedocs.io/en/latest/best_practices/#how-do-we-hash>

    CAST(MD5(NULLIF(CONCAT_WS('||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^')
    ), '^^')) AS TEXT) AS user_pk,

    CAST(MD5(CONCAT_WS('||',
        -- hub PK fields
        COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^'),
        -- payload fields
        COALESCE(NULLIF(UPPER(TRIM(CAST(legal_type AS TEXT))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(district AS TEXT))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(reg_date AS TEXT))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(billing_mode AS TEXT))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(is_vip AS TEXT))), ''), '^^')
    )) AS TEXT) AS user_mdm_hashdiff
FROM
    gosipenkov.ods_user;
