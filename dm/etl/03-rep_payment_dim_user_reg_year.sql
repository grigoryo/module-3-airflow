CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_user_reg_year () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO gosipenkov.rep_payment_dim_user_reg_year
    (
        user_reg_year_name
    )
    SELECT
        DISTINCT user_registered_at_year
    FROM
        gosipenkov.rep_payment_tmp AS tmp
        LEFT JOIN gosipenkov.rep_payment_dim_user_reg_year AS dim
        ON tmp.user_registered_at_year = dim.user_reg_year_name
    WHERE dim.user_reg_year_name IS NULL;
END;
$$;
