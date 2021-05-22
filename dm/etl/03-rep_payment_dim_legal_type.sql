CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_dim_legal_type () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO gosipenkov.rep_payment_dim_legal_type
    (
        legal_type_name
    )
    SELECT
        DISTINCT user_legal_type
    FROM
        gosipenkov.rep_payment_tmp AS tmp
        LEFT JOIN gosipenkov.rep_payment_dim_legal_type AS dim
        ON tmp.user_legal_type = dim.legal_type_name
    WHERE dim.legal_type_name IS NULL;
END;
$$;
