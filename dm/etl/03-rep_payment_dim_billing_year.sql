CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_billing_year () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO gosipenkov.rep_payment_dim_billing_year
    (
        billing_year_name
    )
    SELECT
        DISTINCT payment_billing_period_year
    FROM
        gosipenkov.rep_payment_tmp AS tmp
        LEFT JOIN gosipenkov.rep_payment_dim_billing_year AS dim
        ON tmp.payment_billing_period_year = dim.billing_year_name
    WHERE dim.billing_year_name IS NULL;
END;
$$;
