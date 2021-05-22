CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_tmp (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    -- Факты: платежи.
    -- Текущие данные из сателлитов (Raw Data Mart).
    -- Ось времени: gosipenkov.sat_payment_billing_period.billing_period

    TRUNCATE TABLE gosipenkov.rep_payment_tmp;

    INSERT INTO gosipenkov.rep_payment_tmp
    (
        user_legal_type,
        user_district,
        user_registered_at,
        user_billing_mode,
        user_is_vip,

        payment_billing_period,

        pay_doc_amount,

        -- calculated
        payment_billing_period_year,
        user_registered_at_year
    )
    SELECT
        mu.legal_type,
        mu.district,
        mu.registered_at,
        mu.billing_mode,
        mu.is_vip,

        spbp.billing_period,

        spdd.amount,

        CAST(SUBSTRING(CAST(spbp.billing_period AS TEXT), 1, 4) AS INT) AS payment_billing_period_year,
        CAST(EXTRACT(YEAR FROM mu.registered_at) AS INT) AS user_registered_at_year
    FROM
        (
            gosipenkov.lnk_payment AS lp
            INNER JOIN (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_pk ORDER BY effective_from DESC) AS rownum
                FROM gosipenkov.sat_payment_billing_period
            ) spbp
            ON lp.payment_pk = spbp.payment_pk AND spbp.rownum = 1
        )
        INNER JOIN gosipenkov.hub_user hu
        ON lp.user_pk = hu.user_pk

        INNER JOIN (
            gosipenkov.hub_pay_doc hpd
            INNER JOIN (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY pay_doc_pk ORDER BY effective_from DESC) AS rownum
                FROM gosipenkov.sat_pay_doc_detail
            ) spdd
            ON hpd.pay_doc_pk = spdd.pay_doc_pk AND spdd.rownum = 1
        )
        ON lp.pay_doc_pk = hpd.pay_doc_pk

        INNER JOIN mdm.user AS mu
        ON hu.user_id = mu.id;

END;
$$;
