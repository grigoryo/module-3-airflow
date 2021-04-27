CREATE OR REPLACE FUNCTION gosipenkov.load_sat_payment_billing_period (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH payment_billing_period_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                CAST(MD5(NULLIF(CONCAT_WS('||',
                    COALESCE(NULLIF(UPPER(TRIM(CAST(doc_type AS TEXT))), ''), '^^'),
                    COALESCE(NULLIF(UPPER(TRIM(CAST(doc_num AS TEXT))), ''), '^^'),
                    COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^')
                ), '^^||^^||^^')) AS TEXT) AS payment_pk,
                CURRENT_TIMESTAMP load_date,
                'gosipenkov.ods_payment' record_source,

                pay_date AS effective_from,
                CAST(MD5(NULLIF(CONCAT_WS('||',
                    COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS TEXT))), ''), '^^')
                ), '^^')) AS TEXT) AS payload_hash,

                -- payload
                billing_period,

                ROW_NUMBER() OVER (PARTITION BY payment_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_payment
            WHERE YEAR(pay_date) = p_year
        )
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_payment_billing_period
    (
        payment_pk,
        load_date,
        record_source,

        effective_from,
        payload_hash,

        billing_period
    )
    SELECT
        rec.payment_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.payload_hash,

        rec.billing_period
    FROM
        payment_billing_period_records AS rec
        LEFT JOIN gosipenkov.sat_payment_billing_period AS sat
        ON rec.payload_hash = sat.payload_hash
    WHERE sat.payload_hash IS NULL
END;
$$;
