CREATE OR REPLACE FUNCTION gosipenkov.mdm_load_stg_user () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.stg_user;

    INSERT INTO gosipenkov.stg_user
    (
        id,
        legal_type,
        district,
        registered_at,
        billing_mode,
        is_vip
    )
    SELECT
        id,
        legal_type,
        district,
        registered_at,
        billing_mode,
        is_vip
    FROM mdm.user;

END;
$$;
