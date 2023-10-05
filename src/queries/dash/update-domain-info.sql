--- description: Allow privileged user to insert/update domain_info table
--- Access-Control-Allow-Origin: *
--- url: -
--- ims: -
--- timezone: UTC
--- domainkey: secret

MERGE INTO `helix-225321.helix_reporting.domain_info` AS info
USING (
  SELECT
    @url AS url,
    hostname_prefix,
    readonly
  FROM
    `helix-225321.helix_reporting.domain_keys`
  WHERE
    key_bytes = SHA512(@domainkey)
    AND (
      revoke_date IS NULL
      OR revoke_date > CURRENT_DATE(@timezone)
    )
    AND readonly = FALSE -- key must be read-write to update domain_info
    AND hostname_prefix = ""
) AS validkeys
  ON info.domain = validkeys.url
WHEN MATCHED THEN
  UPDATE SET ims_org_id = @ims
WHEN NOT MATCHED BY TARGET THEN
  INSERT (domain, ims_org_id)
  VALUES (validkeys.url, @ims)
