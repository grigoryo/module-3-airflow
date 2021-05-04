CREATE TABLE gosipenkov.sat_user_detail (
    -- calculated
    user_pk BINARY(16), -- hub_user
    load_date TIMESTAMP,
    record_source TEXT, -- ods_payment

    effective_from TIMESTAMP,
    hashdiff BINARY(16), -- ...hub_user, account, phone

    -- payload
    account TEXT,
    phone TEXT
);
