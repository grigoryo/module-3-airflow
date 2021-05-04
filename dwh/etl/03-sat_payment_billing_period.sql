CREATE OR REPLACE FUNCTION gosipenkov.load_sat_payment_billing_period (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH payment_billing_period_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                payment_pk,
                load_date,
                record_source,

                effective_from,
                payment_billing_period_hashdiff AS hashdiff,

                -- payload
                billing_period,

                ROW_NUMBER() OVER (PARTITION BY payment_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE DATE_PART('YEAR', pay_date) = p_year
        ) AS q
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_payment_billing_period
    (
        payment_pk,
        load_date,
        record_source,

        effective_from,
        hashdiff,

        billing_period
    )
    SELECT
        rec.payment_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.hashdiff,

        rec.billing_period
    FROM
        payment_billing_period_records AS rec
        LEFT JOIN gosipenkov.sat_payment_billing_period AS sat
        ON rec.hashdiff = sat.hashdiff
    WHERE sat.hashdiff IS NULL;
END;
$$;
