--- description: Rotate domain keys
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
--- newkey: -
--- graceperiod: 1
--- expiry: -
DECLARE newkey STRING;

IF EXISTS (
  SELECT
    hostname_prefix,
    key,
    revoke_date
  FROM `helix-225321.helix_reporting.domain_keys`
  WHERE
    key = @domainkey
    AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE(@timezone))
    AND (hostname_prefix = "" OR hostname_prefix = @url)
) THEN
  SET newkey = IF(@newkey = "-", GENERATE_UUID(), @newkey);
  CALL helix_rum . ROTATE_DOMAIN_KEYS (
    @domainkey,
    IF(@url = "-", "", @url),
    @timezone,
    CAST(@graceperiod AS INT64),
    @expiry,
    newkey
  );
END IF;

SELECT
  newkey AS key,
  IF(@url = "-", "", @url) AS hostname_prefix,
  IF(EXISTS (
    SELECT * FROM `helix-225321.helix_reporting.domain_keys`
    WHERE
      key = @domainkey
      AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE(@timezone))
      AND (hostname_prefix = "" OR hostname_prefix = @url)
  ), "success", "failure") AS status,
  IF(@expiry = "-", NULL, @expiry) AS revoke_date
