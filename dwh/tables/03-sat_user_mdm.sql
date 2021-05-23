CREATE TABLE gosipenkov.sat_user_mdm (
    -- calculated
    user_pk TEXT, -- hub_user
    load_date TIMESTAMP,
    record_source TEXT, -- ods_user

    effective_from TIMESTAMP, -- load_date
    hashdiff TEXT, -- ...hub_user, legal_type, district, reg_date, billing_mode, is_vip

    -- payload
    legal_type TEXT,
    district TEXT,
    reg_date DATE,
    billing_mode TEXT,
    is_vip BOOLEAN
);
