--- description: Determine if a given domainkey has access to all domains
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=3600
--- timezone: UTC
--- domainkey: secret

SELECT
  read,
  write
FROM helix_reporting.DOMAINKEY_PRIVS_ALL(@domainkey, @timezone)
