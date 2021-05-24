CREATE OR REPLACE FUNCTION gosipenkov.mdm_load_sat_user_mdm () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH user_mdm_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                user_pk,
                load_date,
                record_source,

                effective_from,
                user_mdm_hashdiff AS hashdiff,

                -- payload
                legal_type,
                district,
                reg_date,
                billing_mode,
                is_vip,

                ROW_NUMBER() OVER (PARTITION BY user_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_v_user
        ) AS q
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_user_mdm
    (
        user_pk,
        load_date,
        record_source,

        effective_from,
        hashdiff,

        legal_type,
        district,
        reg_date,
        billing_mode,
        is_vip
    )
    SELECT
        rec.user_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.hashdiff,

        rec.legal_type,
        rec.district,
        rec.reg_date,
        rec.billing_mode,
        rec.is_vip
    FROM
        user_mdm_records AS rec
        LEFT JOIN gosipenkov.sat_user_mdm AS sat
        ON rec.hashdiff = sat.hashdiff
    WHERE sat.hashdiff IS NULL;
END;
$$;
