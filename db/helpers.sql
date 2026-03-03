SET ROLE proxy_whitelist_owner;

CREATE OR REPLACE FUNCTION proxy_whitelist_app.continueProcessing()
RETURNS BOOLEAN
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  RETURN continue_processing FROM proxy_whitelist_app.app_status LIMIT 1;
END
$$;

CREATE OR REPLACE FUNCTION proxy_whitelist_app.allowProcessing()
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
  UPDATE proxy_whitelist_app.app_status SET continue_processing = True;
END
$$;

--- Helper function to be called from separate transaction
--- (must be committed) to safely stop execution of the application.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.stopProcessing()
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
  UPDATE proxy_whitelist_app.app_status SET continue_processing = False;
END
$$;

CREATE OR REPLACE FUNCTION proxy_whitelist_app.GET_VERSION()
RETURNS TEXT
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  RETURN runtime_hash FROM proxy_whitelist_app.version LIMIT 1;
END;
$$;

CREATE OR REPLACE FUNCTION proxy_whitelist_app.SET_VERSION(_git_hash TEXT)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
  _schema_hash TEXT := (SELECT schema_hash FROM proxy_whitelist_app.version LIMIT 1);
BEGIN
  TRUNCATE TABLE proxy_whitelist_app.version;

  IF _schema_hash IS NULL THEN
    INSERT INTO proxy_whitelist_app.version(schema_hash, runtime_hash) VALUES (_git_hash, _git_hash);
  ELSE
    INSERT INTO proxy_whitelist_app.version(schema_hash, runtime_hash) VALUES (_schema_hash, _git_hash);
  END IF;
END
$$;

RESET ROLE;
