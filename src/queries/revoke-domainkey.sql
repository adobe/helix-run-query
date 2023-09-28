--- description: Rotate domain keys
--- Access-Control-Allow-Origin: *
--- timezone: UTC
--- url: -
--- domainkey: secret
--- revokekey: -
--- graceperiod: 1
DECLARE revokekey STRING;

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
  SET revokekey = IF(@revokekey = "-", "", @revokekey);
  CALL helix_reporting.REVOKE_DOMAIN_KEY( -- noqa: PRS, LT01
    @revokekey,
    IF(@url = "-", "", @url),
    @timezone,
    CAST(@graceperiod AS INT64)
  );
END IF;

SELECT
  revokekey AS key,
  IF(@url = "-", "", @url) AS hostname_prefix,
  IF(EXISTS (
    SELECT * FROM `helix-225321.helix_reporting.domain_keys`
    WHERE
      key_bytes = SHA512(revokekey)
      AND revoke_date IS NOT NULL
      AND (hostname_prefix = "" OR hostname_prefix = @url)
  ), "success", "failure") AS status
