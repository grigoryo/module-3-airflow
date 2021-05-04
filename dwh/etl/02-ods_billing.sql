CREATE OR REPLACE FUNCTION gosipenkov.load_ods_billing (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_billing WHERE DATE_PART('YEAR', created_at) = p_year;

    INSERT INTO gosipenkov.ods_billing
    SELECT
        user_id,
        billing_period,
        service,
        tariff,
        amount,
        created_at,
        -- calculated
        CURRENT_TIMESTAMP AS load_date
    FROM
        gosipenkov.stg_billing
    WHERE
        DATE_PART('YEAR', created_at) = p_year;
END;
$$;
