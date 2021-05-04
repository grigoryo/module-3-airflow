CREATE OR REPLACE FUNCTION gosipenkov.load_lnk_payment (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH payment_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                payment_pk,
                load_date,
                record_source,

                -- payload
                pay_doc_pk,
                user_pk,

                ROW_NUMBER() OVER (PARTITION BY pay_doc_pk ORDER BY effective_from ASC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE DATE_PART('YEAR', pay_date) = p_year
        ) AS q
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.lnk_payment
    (
        payment_pk,
        load_date,
        record_source,

        pay_doc_pk,
        user_pk
    )
    SELECT
        rec.payment_pk,
        rec.load_date,
        rec.record_source,

        rec.pay_doc_pk,
        rec.user_pk
    FROM
        payment_records AS rec
        LEFT JOIN gosipenkov.lnk_payment AS lnk
        ON rec.payment_pk = lnk.payment_pk
    WHERE lnk.payment_pk IS NULL;
END;
$$;
