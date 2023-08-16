--- description: Allow privileged user to insert/update domain_info table
--- Access-Control-Allow-Origin: *
--- url: -
--- ims: -
--- timezone: UTC
--- domainkey: secret

DECLARE result STRING;

IF (
  -- permissions check
  SELECT write FROM helix_reporting.DOMAINKEY_PRIVS_ALL(@domainkey, @timezone)
) THEN
  -- conditionally update or insert IMS org id into domain_info table
  IF EXISTS (SELECT 1 FROM helix_reporting.domain_info WHERE domain = @url) THEN
    UPDATE helix_reporting.domain_info
    SET ims_org_id = @ims
    WHERE domain = @url;
    SET result = '1 row updated';
  ELSE
    INSERT INTO helix_reporting.domain_info (domain, ims_org_id) VALUES (@url, @ims);
    SET result = '1 row inserted';
  END IF;
ELSE
  SET result = 'domainkey not valid';
END IF;

SELECT result;
