CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_billing_mode () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO gosipenkov.rep_payment_dim_billing_mode
    (
        billing_mode_name
    )
    SELECT
        DISTINCT user_billing_mode
    FROM
        gosipenkov.rep_payment_tmp AS tmp
        LEFT JOIN gosipenkov.rep_payment_dim_billing_mode AS dim
        ON tmp.user_billing_mode = dim.billing_mode_name
    WHERE dim.billing_mode_name IS NULL;
END;
$$;
