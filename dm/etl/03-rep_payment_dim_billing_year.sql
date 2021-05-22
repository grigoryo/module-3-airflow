CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_billing_year () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_dim_billing_year;

    INSERT INTO gosipenkov.rep_payment_dim_billing_year
    (
        billing_year_id,
        billing_year_name
    )
    SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY payment_billing_period_year ASC),
        payment_billing_period_year
    FROM gosipenkov.rep_payment_tmp
    ORDER BY 1 ASC;
END;
$$;
