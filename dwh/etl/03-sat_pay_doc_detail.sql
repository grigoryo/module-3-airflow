CREATE OR REPLACE FUNCTION gosipenkov.load_sat_pay_doc_detail (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH pay_doc_detail_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                pay_doc_pk,
                load_date,
                record_source,

                effective_from,
                pay_doc_detail_hashdiff AS hashdiff,

                -- payload
                pay_date,
                amount,

                ROW_NUMBER() OVER (PARTITION BY pay_doc_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE DATE_PART('YEAR', pay_date) = p_year
        ) AS q
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_pay_doc_detail
    (
        pay_doc_pk,
        load_date,
        record_source,

        effective_from,
        hashdiff,

        pay_date,
        amount
    )
    SELECT
        rec.pay_doc_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.hashdiff,

        rec.pay_date,
        rec.amount
    FROM
        pay_doc_detail_records AS rec
        LEFT JOIN gosipenkov.sat_pay_doc_detail AS sat
        ON rec.hashdiff = sat.hashdiff
    WHERE sat.hashdiff IS NULL;
END;
$$;
