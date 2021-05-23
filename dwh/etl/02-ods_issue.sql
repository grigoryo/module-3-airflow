CREATE OR REPLACE FUNCTION gosipenkov.load_ods_issue (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_issue WHERE DATE_PART('YEAR', end_date) = p_year;

    INSERT INTO gosipenkov.ods_issue
    (
        user_id,
        start_time,
        end_time,
        title,
        description,
        service,
        load_date
    )
    SELECT
        user_id,
        CAST(start_time AS TIMESTAMP) AS start_time,
        CAST(end_time AS TIMESTAMP) AS end_time,
        title,
        description,
        service,
        -- calculated
        CURRENT_TIMESTAMP AS load_date
    FROM
        gosipenkov.stg_issue
    WHERE
        DATE_PART('YEAR', end_date) = p_year;
END;
$$;
