CREATE OR REPLACE FUNCTION gosipenkov.load_ods_billing (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_billing WHERE DATE_PART('YEAR', created_at) = p_year;

    INSERT INTO gosipenkov.ods_billing
    (
        user_id,
        billing_period,
        service,
        tariff,
        amount,
        created_at,
        load_date
    )
    SELECT
        user_id,
        CAST(SUBSTRING(billing_period, 1, 4) || SUBSTRING(billing_period, 6, 2) AS INT) AS billing_period,
        service,
        tariff,
        CAST(sum AS DECIMAL) AS amount,
        CAST(created_at AS DATE) AS created_at,
        -- calculated
        CURRENT_TIMESTAMP AS load_date
    FROM
        gosipenkov.stg_billing
    WHERE
        DATE_PART('YEAR', created_at) = p_year;
END;
$$;
