CREATE TABLE gosipenkov.rep_payment_tmp (
    user_legal_type TEXT, -- mdm.user.legal_type
    user_district TEXT, -- mdm.user.district
    user_registered_at_year TEXT, -- calculated from mdm.user.registered_at (TIMESTAMP)
    user_billing_mode TEXT, -- mdm.user.billing_mode
    user_is_vip BOOLEAN, -- mdm.user.is_vip
    payment_billing_period_year TEXT, -- calculated from billing_period (INT)
    sum_pay_doc_amount DECIMAL -- SUM of gosipenkov.sat_pay_doc_detail.amount
);
