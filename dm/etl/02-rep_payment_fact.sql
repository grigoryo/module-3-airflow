CREATE OR REPLACE FUNCTION gosipenkov.truncate_rep_payment_fact () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE gosipenkov.rep_payment_fact;
END;
$$;
