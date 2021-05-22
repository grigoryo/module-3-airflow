CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_legal_type () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_dim_legal_type;

    INSERT INTO gosipenkov.rep_payment_dim_legal_type
    (
        legal_type_id,
        legal_type_name
    )
    SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY user_legal_type ASC),
        user_legal_type
    FROM gosipenkov.rep_payment_tmp
    ORDER BY 1 ASC;
END;
$$;
