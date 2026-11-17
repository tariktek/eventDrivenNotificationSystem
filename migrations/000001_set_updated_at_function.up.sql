-- Shared trigger function: keeps updated_at current on every UPDATE so the
-- application never has to remember to set it.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;
