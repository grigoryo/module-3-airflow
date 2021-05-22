CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_billing_mode () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_dim_billing_mode;

    INSERT INTO gosipenkov.rep_payment_dim_billing_mode
    (
        billing_mode_id,
        billing_mode_name
    )
    SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY user_billing_mode ASC),
        user_billing_mode
    FROM gosipenkov.rep_payment_tmp
    ORDER BY 1 ASC;
END;
$$;
