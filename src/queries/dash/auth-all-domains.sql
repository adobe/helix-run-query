--- description: Determine if a given domainkey has access to all domains
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=3600
--- timezone: UTC
--- domainkey: secret

SELECT COALESCE(
  (
    SELECT IF(hostname_prefix = '', true, false)
    FROM
      `helix-225321.helix_reporting.domain_keys`
    WHERE
      key_bytes = SHA512(@domainkey)
      AND (
        revoke_date IS NULL
        OR revoke_date > CURRENT_DATE(@timezone)
      )
  ),
  false
) AS auth
