CREATE TABLE gosipenkov.hub_user (
    -- calculated
    user_pk BINARY(16), -- user_id
    load_date TIMESTAMP,
    record_source TEXT, -- ods_payment

    -- payload
    user_id INT
);
