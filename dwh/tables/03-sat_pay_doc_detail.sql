CREATE TABLE gosipenkov.sat_pay_doc_detail (
    -- calculated
    pay_doc_pk TEXT, -- hub_pay_doc
    load_date TIMESTAMP,
    record_source TEXT, -- ods_payment

    effective_from TIMESTAMP,
    payload_hash TEXT, -- pay_date, amount

    -- payload
    pay_date DATE,
    amount DECIMAL
);
