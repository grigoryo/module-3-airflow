CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_district () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_dim_district;

    INSERT INTO gosipenkov.rep_payment_dim_district
    (
        district_id,
        district_name
    )
    SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY user_district ASC),
        user_district
    FROM gosipenkov.rep_payment_tmp
    ORDER BY 1 ASC;
END;
$$;
