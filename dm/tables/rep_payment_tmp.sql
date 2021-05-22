CREATE TABLE gosipenkov.rep_payment_tmp (
    user_legal_type TEXT,
    user_district TEXT,
    user_registered_at TIMESTAMP,
    user_billing_mode TEXT,
    user_is_vip BOOLEAN,
    payment_billing_period INT,
    pay_doc_amount DECIMAL,
    -- calculated
    payment_billing_period_year INT,
    user_registered_at_year INT
);
