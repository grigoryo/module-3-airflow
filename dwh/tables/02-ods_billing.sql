CREATE TABLE gosipenkov.ods_billing (
    user_id INT,
    billing_period INT,
    service TEXT,
    tariff TEXT,
    amount DECIMAL,
    created_at DATE,
    load_date TIMESTAMP
);
