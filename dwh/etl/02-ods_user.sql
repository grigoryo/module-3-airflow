CREATE OR REPLACE FUNCTION gosipenkov.load_ods_user () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO gosipenkov.ods_user
    (
        user_id,
        legal_type,
        district,
        reg_date,
        billing_mode,
        is_vip,
        load_date
    )
    SELECT
        id,
        legal_type,
        district,
        CAST(registered_at AS DATE) AS reg_date,
        billing_mode,
        is_vip,
        -- calculated
        CURRENT_TIMESTAMP AS load_date
    FROM
        gosipenkov.stg_user;
END;
$$;
