CREATE OR REPLACE FUNCTION gosipenkov.load_rep_payment_tmp () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    -- Факты: платежи.
    -- Текущие данные из сателлитов (Raw Data Mart).
    -- Ось времени: gosipenkov.sat_payment_billing_period.billing_period

    TRUNCATE TABLE gosipenkov.rep_payment_tmp;

    INSERT INTO gosipenkov.rep_payment_tmp
    (
        user_legal_type,
        user_district,
        user_registered_at_year, -- calculated
        user_billing_mode,
        user_is_vip,
        payment_billing_period_year, -- calculated
        sum_pay_doc_amount
    )
    SELECT
        r.user_legal_type,
        r.user_district,
        r.user_registered_at_year,
        r.user_billing_mode,
        r.user_is_vip,
        r.payment_billing_period_year,
        SUM(r.pay_doc_amount) AS sum_pay_doc_amount
    FROM (
        SELECT
            mu.legal_type AS user_legal_type,
            mu.district AS user_district,
            CAST(EXTRACT(YEAR FROM mu.registered_at) AS TEXT) AS user_registered_at_year,
            mu.billing_mode AS user_billing_mode,
            mu.is_vip AS user_is_vip,

            SUBSTRING(CAST(spbp.billing_period AS TEXT), 1, 4) AS payment_billing_period_year,

            spdd.amount AS pay_doc_amount
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
            ON hu.user_id = mu.id
    ) AS r
    GROUP BY
        r.user_legal_type,
        r.user_district,
        r.user_registered_at_year,
        r.user_billing_mode,
        r.user_is_vip,
        r.payment_billing_period_year;

END;
$$;
