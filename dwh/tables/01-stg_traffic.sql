CREATE EXTERNAL TABLE gosipenkov.stg_traffic (
    user_id INT,
    timestamp BIGINT,
    device_id TEXT,
    device_ip_addr TEXT,
    bytes_sent INT,
    bytes_received INT
)
LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/traffic/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

/*
user_id   timestamp       device_id    device_ip_addr   bytes_sent   bytes_received
I         BIGINT          S            S                I            I
-----------------------------------------------------------------------------------
10200     1381521307000   d031558589   57.61.111.162    253790510    2122103004
10970     1388364052000   d090826509   157.174.174.64   2035077022   1111975182
10070     1375285104000   d060962271   136.96.124.116   1447319199   562280166
*/
