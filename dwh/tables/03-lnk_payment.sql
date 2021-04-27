CREATE TABLE gosipenkov.lnk_payment (
    -- calculated
    payment_pk TEXT, -- pay_doc_pk, user_pk
    load_date TIMESTAMP,
    record_source TEXT, -- ods_payment

    -- payload
    pay_doc_pk TEXT,
    user_pk TEXT
);
