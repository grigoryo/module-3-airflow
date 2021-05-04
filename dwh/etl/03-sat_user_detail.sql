CREATE OR REPLACE FUNCTION gosipenkov.load_sat_user_detail (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH user_detail_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                user_pk,
                CURRENT_TIMESTAMP load_date,
                'gosipenkov.ods_v_payment' record_source,

                pay_date AS effective_from,
                user_detail_payload_hash AS payload_hash,

                -- payload
                account,
                phone,

                ROW_NUMBER() OVER (PARTITION BY user_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE YEAR(pay_date) = p_year
        )
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_user_detail
    (
        user_pk,
        load_date,
        record_source,

        effective_from,
        payload_hash,

        account,
        phone
    )
    SELECT
        rec.user_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.payload_hash,

        rec.account,
        rec.phone
    FROM
        user_detail_records AS rec
        LEFT JOIN gosipenkov.sat_user_detail AS sat
        ON rec.payload_hash = sat.payload_hash
    WHERE sat.payload_hash IS NULL
END;
$$;
