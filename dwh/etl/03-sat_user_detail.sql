CREATE OR REPLACE FUNCTION gosipenkov.load_sat_user_detail (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH user_detail_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                user_pk,
                load_date,
                record_source,

                effective_from,
                user_detail_hashdiff AS hashdiff,

                -- payload
                account,
                phone,

                ROW_NUMBER() OVER (PARTITION BY user_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE DATE_PART('YEAR', pay_date) = p_year
        ) AS q
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_user_detail
    (
        user_pk,
        load_date,
        record_source,

        effective_from,
        hashdiff,

        account,
        phone
    )
    SELECT
        rec.user_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.hashdiff,

        rec.account,
        rec.phone
    FROM
        user_detail_records AS rec
        LEFT JOIN gosipenkov.sat_user_detail AS sat
        ON rec.hashdiff = sat.hashdiff
    WHERE sat.hashdiff IS NULL;
END;
$$;
