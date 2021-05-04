CREATE OR REPLACE FUNCTION gosipenkov.load_sat_pay_doc_detail (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH pay_doc_detail_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                pay_doc_pk,
                CURRENT_TIMESTAMP load_date,
                'gosipenkov.ods_v_payment' record_source,

                pay_date AS effective_from,
                pay_doc_detail_payload_hash AS payload_hash,

                -- payload
                pay_date,
                amount,

                ROW_NUMBER() OVER (PARTITION BY pay_doc_pk ORDER BY effective_from DESC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE YEAR(pay_date) = p_year
        )
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.sat_pay_doc_detail
    (
        pay_doc_pk,
        load_date,
        record_source,

        effective_from,
        payload_hash,

        pay_date,
        amount
    )
    SELECT
        rec.pay_doc_pk,
        rec.load_date,
        rec.record_source,

        rec.effective_from,
        rec.payload_hash,

        rec.pay_date,
        rec.amount
    FROM
        pay_doc_detail_records AS rec
        LEFT JOIN gosipenkov.sat_pay_doc_detail AS sat
        ON rec.payload_hash = sat.payload_hash
    WHERE sat.payload_hash IS NULL
END;
$$;
