CREATE OR REPLACE FUNCTION gosipenkov.load_ods_payment (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_payment WHERE YEAR(pay_date) = p_year;

    INSERT INTO gosipenkov.ods_payment
    SELECT
        user_id,
        doc_type,
        doc_num,
        account,
        phone,
        billing_period,
        pay_date,
        amount,
        -- calculated
        CURRENT_TIMESTAMP load_date
    FROM
        gosipenkov.stg_payment
    WHERE
        YEAR(pay_date) = p_year;
END;
$$;
