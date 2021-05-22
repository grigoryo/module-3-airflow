CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_user_reg_year () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_dim_user_reg_year;

    INSERT INTO gosipenkov.rep_payment_dim_user_reg_year
    (
        user_reg_year_id,
        user_reg_year_name
    )
    SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY user_reg_year_name ASC),
        user_reg_year_name
    FROM gosipenkov.rep_payment_tmp
    ORDER BY 1 ASC;
END;
$$;
