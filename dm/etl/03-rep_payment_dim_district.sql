CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_district () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO gosipenkov.rep_payment_dim_district
    (
        district_name
    )
    SELECT
        DISTINCT user_district
    FROM
        gosipenkov.rep_payment_tmp AS tmp
        LEFT JOIN gosipenkov.rep_payment_dim_district AS dim
        ON tmp.user_district = dim.district_name
    WHERE dim.district_name IS NULL;
END;
$$;
