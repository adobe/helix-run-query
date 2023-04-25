--- description: Rotate domain keys
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
--- newkey: -
--- graceperiod: 1
--- expiry: -
--- readonly: true
DECLARE newkey STRING;

IF EXISTS (
  SELECT
    hostname_prefix,
    key_bytes,
    revoke_date
  FROM `helix-225321.helix_reporting.domain_keys`
  WHERE
    key_bytes = SHA512(@domainkey)
    AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE(@timezone))
    AND (hostname_prefix = "" OR hostname_prefix = @url)
    AND readonly = FALSE
) THEN
  SET newkey = IF(@newkey = "-", GENERATE_UUID(), @newkey);
  CALL helix_rum . ROTATE_DOMAIN_KEYS(
    @domainkey,
    IF(@url = "-", "", @url),
    @timezone,
    CAST(@graceperiod AS INT64),
    @expiry,
    newkey,
    CAST(@readonly AS BOOL)
  );
END IF;

SELECT
  newkey AS key,
  IF(@url = "-", "", @url) AS hostname_prefix,
  IF(EXISTS (
    SELECT * FROM `helix-225321.helix_reporting.domain_keys`
    WHERE
      key_bytes = SHA512(@domainkey)
      AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE(@timezone))
      AND (hostname_prefix = "" OR hostname_prefix = @url)
  ), "success", "failure") AS status,
  IF(@expiry = "-", NULL, @expiry) AS revoke_date
