CREATE TABLE gosipenkov.rep_payment_fact (
    legal_type_id INT,
    district_id INT,
    user_reg_year_id INT,
    billing_mode_id INT,
    is_vip BOOLEAN,
    billing_year_id INT,
    amount DECIMAL,
    FOREIGN KEY(legal_type_id) REFERENCES gosipenkov.rep_payment_dim_legal_type(legal_type_id),
    FOREIGN KEY(district_id) REFERENCES gosipenkov.rep_payment_dim_district(district_id),
    FOREIGN KEY(user_reg_year_id) REFERENCES gosipenkov.rep_payment_dim_user_reg_year(user_reg_year_id),
    FOREIGN KEY(billing_mode_id) REFERENCES gosipenkov.rep_payment_dim_billing_mode(billing_mode_id),
    FOREIGN KEY(billing_year_id) REFERENCES gosipenkov.rep_payment_dim_billing_year(billing_year_id)
);
