CREATE OR REPLACE FUNCTION gosipenkov.load_hub_pay_doc (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH pay_doc_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                pay_doc_pk,
                load_date,
                record_source,

                -- payload
                doc_type,
                doc_num,

                ROW_NUMBER() OVER (PARTITION BY pay_doc_pk ORDER BY effective_from ASC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE YEAR(pay_date) = p_year
        ) AS q
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.hub_pay_doc
    (
        pay_doc_pk,
        load_date,
        record_source,

        doc_type,
        doc_num
    )
    SELECT
        rec.pay_doc_pk,
        rec.load_date,
        rec.record_source,

        rec.doc_type,
        rec.doc_num
    FROM
        pay_doc_records AS rec
        LEFT JOIN gosipenkov.hub_pay_doc AS hub
        ON rec.pay_doc_pk = hub.pay_doc_pk
    WHERE hub.pay_doc_pk IS NULL;
END;
$$;
