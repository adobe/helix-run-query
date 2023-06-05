--- description: Number of sidekick users in a time period
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=86400
--- startdate: 2023-05-01
--- enddate: 2023-05-30
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
WITH validkeys AS (
  SELECT readonly
  FROM `helix-225321.helix_reporting.domain_keys`
  WHERE
    key_bytes = SHA512(@domainkey)
    AND (revoke_date IS NULL OR revoke_date > CURRENT_DATE('UTC'))
    AND not readonly
)
SELECT
  readonly,
  *
FROM `helix-225321.mrosier_test.sk-daily-users`, validkeys
WHERE 
TIMESTAMP(@startdate, helix_rum.CLEAN_TIMEZONE(@timezone)) <= TIMESTAMP(day, helix_rum.CLEAN_TIMEZONE(@timezone)) AND
TIMESTAMP(@enddate, helix_rum.CLEAN_TIMEZONE(@timezone)) >= TIMESTAMP(day, helix_rum.CLEAN_TIMEZONE(@timezone)) AND 
owner_repo = @owner_repo AND
not readonly
order by owner_repo, day desc