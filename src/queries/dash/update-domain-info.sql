--- description: Allow privileged user to insert/update domain_info table
--- Access-Control-Allow-Origin: *
--- url: -
--- ims: -
--- timezone: UTC
--- domainkey: secret

DECLARE result STRING;
CALL `helix_reporting.UPDATE_DOMAIN_INFO` (
  @domainkey,
  @timezone,
  @url,
  @ims,
  result
);
SELECT result;
