CREATE EXTERNAL TABLE gosipenkov.stg_billing (
    user_id INT,
    billing_period TEXT,
    service TEXT,
    tariff TEXT,
    sum TEXT,
    created_at TEXT
)
LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/billing/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

/*
user_id   billing_period   service             tariff         sum    created_at
INT       STR              STR                 STR            STR    STR
-------------------------------------------------------------------------------
10550     2013-04          Домашний интернет   Выгодный 300   4912   2013-05-11
10560     2013-03          Домашний интернет   Базовый 200    2690   2013-04-20
10050     2013-02          Домашний интернет   Выгодный 300   9315   2013-03-01
*/
