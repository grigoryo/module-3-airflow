CREATE EXTERNAL TABLE gosipenkov.stg_issue (
    user_id TEXT,
    start_time TEXT,
    end_time TEXT,
    title TEXT,
    description TEXT,
    service TEXT
)
LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/issue/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

/*
user_id   start_time            end_time              title            description      service
S         S                     S                     S                S                S
---------------------------------------------------------------------------------------------------------
10180     2013-08-18 18:42:06   2013-08-20 03:42:06   DFmddkbupTNPCm   HTpRudTGrnMNBq   Цифровое ТВ
11020     2013-03-29 12:24:36   2013-04-01 01:24:36   gZGhWuWZbdbeBi   VXDkkqxDSUrQRR   Цифровое ТВ
10340     2013-12-27 13:13:33   2013-12-29 15:13:33   skzMRwsldPHMVw   GCTZAUIhYHziEP   Домашний интернет
*/
