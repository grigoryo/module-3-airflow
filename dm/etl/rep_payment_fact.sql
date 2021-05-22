CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_fact () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_fact;

    INSERT INTO gosipenkov.rep_payment_fact
    (
        legal_type_id,
        district_id,
        user_reg_year_id,
        billing_mode_id,
        is_vip,
        billing_year_id,
        amount
    )
    SELECT
        dlt.legal_type_id,
        dd.district_id,
        dury.user_reg_year_id,
        dbm.billing_mode_id,
        tmp.user_is_vip AS is_vip,
        dby.billing_year_id,
        tmp.sum_pay_doc_amount AS amount
    FROM
        gosipenkov.rep_payment_tmp AS tmp

        INNER JOIN gosipenkov.rep_payment_dim_legal_type AS dlt
        ON tmp.user_legal_type = dlt.legal_type_name

        INNER JOIN gosipenkov.rep_payment_dim_district AS dd
        ON tmp.user_district = dd.district_name

        INNER JOIN gosipenkov.rep_payment_dim_user_reg_year AS dury
        ON tmp.user_registered_at_year = dury.user_reg_year_name

        INNER JOIN gosipenkov.rep_payment_dim_billing_mode AS dbm
        ON tmp.user_billing_mode = dbm.billing_mode_name

        INNER JOIN gosipenkov.rep_payment_dim_billing_year AS dby
        ON tmp.payment_billing_period_year = dby.billing_year_name;
END;
$$;
