CREATE TABLE gosipenkov.hub_pay_doc (
    -- calculated
    pay_doc_pk BINARY(16), -- doc_type, doc_num
    load_date TIMESTAMP,
    record_source TEXT, -- ods_payment

    -- payload
    doc_type TEXT,
    doc_num INT
);
