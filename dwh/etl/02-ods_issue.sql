CREATE OR REPLACE FUNCTION gosipenkov.load_ods_issue (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM gosipenkov.ods_issue WHERE DATE_PART('YEAR', end_date) = p_year;

    INSERT INTO gosipenkov.ods_issue
    SELECT
        user_id,
        start_date,
        end_date,
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
