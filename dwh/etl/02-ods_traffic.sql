CREATE OR REPLACE FUNCTION gosipenkov.load_ods_traffic (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_traffic WHERE DATE_PART('YEAR', TO_TIMESTAMP(unixtime)) = p_year;

    INSERT INTO gosipenkov.ods_traffic
    SELECT
        user_id,
        unixtime,
        device_id,
        device_ip_addr,
        bytes_sent,
        bytes_received,
        -- calculated
        CURRENT_TIMESTAMP AS load_date
    FROM
        gosipenkov.stg_traffic
    WHERE
        DATE_PART('YEAR', TO_TIMESTAMP(unixtime)) = p_year;
END;
$$;
