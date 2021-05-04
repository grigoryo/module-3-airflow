CREATE OR REPLACE FUNCTION gosipenkov.load_hub_user (p_year INT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    WITH user_records AS (
        SELECT *
        FROM (
            SELECT
                -- calculated
                user_pk,
                load_date,
                record_source,

                -- payload
                user_id,

                ROW_NUMBER() OVER (PARTITION BY user_pk ORDER BY effective_from ASC) AS rownum
            FROM gosipenkov.ods_v_payment
            WHERE YEAR(pay_date) = p_year
        )
        WHERE rownum = 1
    )
    INSERT INTO gosipenkov.hub_user
    (
        user_pk,
        load_date,
        record_source,

        user_id
    )
    SELECT
        rec.user_pk,
        rec.load_date,
        rec.record_source,

        rec.user_id
    FROM
        user_records AS rec
        LEFT JOIN gosipenkov.hub_user AS hub
        ON rec.user_pk = hub.user_pk
    WHERE hub.user_pk IS NULL
END;
$$;
