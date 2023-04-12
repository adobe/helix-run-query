--- description: Rotate domain keys
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
--- graceperiod: 1
--- expiry: -
DECLARE outnewkey STRING;

IF EXISTS(
    SELECT * FROM `helix-225321.helix_reporting.domain_keys`
    WHERE key = @domainkey AND
      (revoke_date IS NULL
      OR revoke_date > CURRENT_DATE(@timezone)) and
    (hostname_prefix = "" OR hostname_prefix = @url)
  ) THEN
  CALL helix_rum.ROTATE_DOMAIN_KEYS(
  @domainkey,
  @url,
  @timezone,
  CAST(@graceperiod AS INT64),
  @expiry,
  outnewkey);
END IF;

SELECT 
  @url AS hostname_prefix,
  IF(EXISTS(
    SELECT * FROM `helix-225321.helix_reporting.domain_keys`
    WHERE key = @domainkey AND
      (revoke_date IS NULL
      OR revoke_date > CURRENT_DATE(@timezone)) and
    (hostname_prefix = "" OR hostname_prefix = @url)
  ), "success", "failure") AS status,
  outnewkey AS key,
  IF(@expiry = "-", NULL, @expiry) AS revoke_date