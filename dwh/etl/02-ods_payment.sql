CREATE OR REPLACE FUNCTION gosipenkov.load_ods_payment (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_payment WHERE DATE_PART('YEAR', pay_date) = p_year;

    INSERT INTO gosipenkov.ods_payment
    (
        user_id,
        doc_type,
        doc_num,
        account,
        phone,
        billing_period,
        pay_date,
        amount,
        load_date
    )
    SELECT
        user_id,
        pay_doc_type AS doc_type,
        pay_doc_num AS doc_num,
        account,
        phone,
        CAST(SUBSTRING(billing_period, 1, 4) || SUBSTRING(billing_period, 6, 2) AS INT) AS billing_period,
        CAST(pay_date AS DATE) AS pay_date,
        CAST(sum AS DECIMAL) AS amount,
        -- calculated
        CURRENT_TIMESTAMP AS load_date
    FROM
        gosipenkov.stg_payment
    WHERE
        DATE_PART('YEAR', pay_date) = p_year;
END;
$$;
