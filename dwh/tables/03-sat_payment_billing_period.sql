CREATE TABLE gosipenkov.sat_payment_billing_period (
    -- calculated
    payment_pk BINARY(16), -- lnk_payment
    load_date TIMESTAMP,
    record_source TEXT, -- ods_payment

    effective_from TIMESTAMP,
    hashdiff BINARY(16), -- ...lnk_payment, billing_period

    -- payload
    billing_period INT
);
