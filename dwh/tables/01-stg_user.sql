CREATE TABLE gosipenkov.stg_user (
    id INT,
    legal_type TEXT,
    district TEXT,
    registered_at TIMESTAMP,
    billing_mode TEXT,
    is_vip BOOLEAN
);

/*
id      legal_type   district   registered_at         billing_mode   is_vip
I       S            S          DT                    S              B
---------------------------------------------------------------------------
10410   Физлицо      ЦФО        2012-12-26 00:00:00   Предоплатный   TRUE
10780   Юрлицо       ЦФО        2010-12-23 00:00:00   Предоплатный   FALSE
11090   Физлицо      ЦФО        2010-03-11 00:00:00   Предоплатный   FALSE
*/
