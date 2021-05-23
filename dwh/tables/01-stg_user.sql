CREATE TABLE gosipenkov.stg_user (
    id INTEGER NOT NULL,
    legal_type TEXT,
    district TEXT,
    registered_at TIMESTAMP WITHOUT TIME ZONE,
    billing_mode TEXT,
    is_vip BOOLEAN NOT NULL
);

/*
id      legal_type   district   registered_at         billing_mode   is_vip
I nn    S            S          TS WO TZ              S              B nn
---------------------------------------------------------------------------
10410   Физлицо      ЦФО        2012-12-26 00:00:00   Предоплатный   TRUE
10780   Юрлицо       ЦФО        2010-12-23 00:00:00   Предоплатный   FALSE
11090   Физлицо      ЦФО        2010-03-11 00:00:00   Предоплатный   FALSE
*/
